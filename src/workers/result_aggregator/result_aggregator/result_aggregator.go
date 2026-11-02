package result_aggregator

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	qresults "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
)

type Config struct {
	MomHost      string
	MomPort      int
	Q1InputQueue string
	Q2InputQueue string
	OutputQueue  string
}

type clientState struct {
	q1Results map[string]*messages.Q1Result
	q2Results map[string]*messages.Q2Result
	q1Done    bool
	q2Done    bool
	finalSent bool
}

type Worker struct {
	q1Input middleware.Middleware
	q2Input middleware.Middleware
	output  middleware.Middleware

	mu      sync.Mutex
	clients map[uint64]*clientState
}

func New(config Config) (*Worker, error) {
	settings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	q1Input, err := middleware.CreateQueueMiddleware(config.Q1InputQueue, settings)
	if err != nil {
		return nil, err
	}
	q2Input, err := middleware.CreateQueueMiddleware(config.Q2InputQueue, settings)
	if err != nil {
		q1Input.Close()
		return nil, err
	}
	output, err := middleware.CreateQueueMiddleware(config.OutputQueue, settings)
	if err != nil {
		q1Input.Close()
		q2Input.Close()
		return nil, err
	}

	return &Worker{
		q1Input: q1Input,
		q2Input: q2Input,
		output:  output,
		clients: make(map[uint64]*clientState),
	}, nil
}

func (w *Worker) Run() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	go func() {
		defer waitGroup.Done()
		if err := w.q1Input.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			w.handleQ1(msg, ack, nack)
		}); err != nil {
			slog.Error("Q1 consumer stopped", "err", err)
		}
	}()
	go func() {
		defer waitGroup.Done()
		if err := w.q2Input.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			w.handleQ2(msg, ack, nack)
		}); err != nil {
			slog.Error("Q2 consumer stopped", "err", err)
		}
	}()

	<-signals
	w.q1Input.StopConsuming()
	w.q2Input.StopConsuming()
	waitGroup.Wait()
	w.shutdown()
}

func (w *Worker) shutdown() {
	if err := w.q1Input.Close(); err != nil {
		slog.Error("While closing Q1 input queue", "err", err)
	}
	if err := w.q2Input.Close(); err != nil {
		slog.Error("While closing Q2 input queue", "err", err)
	}
	if err := w.output.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}

func (w *Worker) handleQ1(msg middleware.Message, ack, nack func()) {
	if msg.MsgCode == messages.EOFCode {
		if err := w.finishQ1(msg.ClientID); err != nil {
			slog.Error("While finishing Q1", "err", err)
			nack()
			return
		}
		ack()
		return
	}
	if msg.MsgCode != messages.Q1ResultCode {
		nack()
		return
	}

	batch, err := messages.Q1ResultBatchFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Error("While parsing Q1 results", "err", err)
		nack()
		return
	}
	w.mu.Lock()
	state := w.getClient(msg.ClientID)
	for _, result := range batch.Items {
		state.q1Results[result.ArticleID] = result
	}
	w.mu.Unlock()
	ack()
}

func (w *Worker) handleQ2(msg middleware.Message, ack, nack func()) {
	if msg.MsgCode == messages.EOFCode {
		if err := w.finishQ2(msg.ClientID); err != nil {
			slog.Error("While finishing Q2", "err", err)
			nack()
			return
		}
		ack()
		return
	}
	if msg.MsgCode != messages.Q2ResultCode {
		nack()
		return
	}

	batch, err := messages.Q2ResultBatchFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Error("While parsing Q2 results", "err", err)
		nack()
		return
	}
	w.mu.Lock()
	state := w.getClient(msg.ClientID)
	for _, result := range batch.Items {
		state.q2Results[result.ArticleID] = result
	}
	w.mu.Unlock()
	ack()
}

func (w *Worker) getClient(clientID uint64) *clientState {
	state, ok := w.clients[clientID]
	if !ok {
		state = &clientState{
			q1Results: make(map[string]*messages.Q1Result),
			q2Results: make(map[string]*messages.Q2Result),
		}
		w.clients[clientID] = state
	}
	return state
}

func (w *Worker) finishQ1(clientID uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.getClient(clientID)
	if state.q1Done {
		return w.finishClientIfDone(clientID, state)
	}

	batch := generic.NewEmptyBatch[*messages.Q1Result](len(state.q1Results))
	for _, result := range state.q1Results {
		batch.Add(result)
	}
	if err := w.publishQueryResult(clientID, qresults.Q1Code, &batch); err != nil {
		return err
	}
	state.q1Done = true
	return w.finishClientIfDone(clientID, state)
}

func (w *Worker) finishQ2(clientID uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.getClient(clientID)
	if state.q2Done {
		return w.finishClientIfDone(clientID, state)
	}

	batch := generic.NewEmptyBatch[*messages.Q2Result](len(state.q2Results))
	for _, result := range state.q2Results {
		batch.Add(result)
	}
	if err := w.publishQueryResult(clientID, qresults.Q2Code, &batch); err != nil {
		return err
	}
	state.q2Done = true
	return w.finishClientIfDone(clientID, state)
}

func (w *Worker) publishQueryResult(clientID uint64, queryCode qresults.QueryResultCode, payload generic.SerializableMessage) error {
	payloadBytes, err := payload.ToBytes()
	if err != nil {
		return err
	}
	result := qresults.QueryResult{
		QueryCode:     queryCode,
		ResultPayload: payloadBytes,
	}
	resultBytes, err := result.ToBytes()
	if err != nil {
		return err
	}
	message := middleware.Message{
		ClientID: clientID,
		MsgCode:  messages.ResultCode,
		Body:     string(resultBytes),
	}
	if err := w.output.Send(message); err != nil {
		return fmt.Errorf("sending query result: %w", err)
	}
	return nil
}

func (w *Worker) finishClientIfDone(clientID uint64, state *clientState) error {
	if !state.q1Done || !state.q2Done || state.finalSent {
		return nil
	}
	message := middleware.Message{
		ClientID: clientID,
		MsgCode:  messages.EOFCode,
		Body:     string([]byte{}),
	}
	if err := w.output.Send(message); err != nil {
		return fmt.Errorf("sending final EOF: %w", err)
	}
	state.finalSent = true
	delete(w.clients, clientID)
	return nil
}
