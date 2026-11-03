package sharedwordsjoiner

import (
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	qresults "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type SharedWordsJoinerConfig struct {
	MomHost                     string
	MomPort                     int
	InputQueue                  string
	OutputQueue                 string
	Q3EofOutputQueue            string
	SharedWordsAggregatorAmount int
}

type SharedWordsJoinerWorker struct {
	inputQueue   middleware.Middleware
	outputQueue  middleware.Middleware
	q3EofOutput  middleware.Middleware
	clientStates map[uint64]*ClientState
	lock         sync.Mutex
	shardsAmount int
}

// ClientState acumula las palabras de los parciales y cuenta los EOF recibidos de los shards.
type ClientState struct {
	words    map[string]struct{}
	eofCount int
}

func New(config SharedWordsJoinerConfig) (*SharedWordsJoinerWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	q3EofOutput, err := middleware.CreateQueueMiddleware(config.Q3EofOutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, err
	}

	return &SharedWordsJoinerWorker{
		inputQueue:   inputQueue,
		outputQueue:  outputQueue,
		q3EofOutput:  q3EofOutput,
		clientStates: map[uint64]*ClientState{},
		lock:         sync.Mutex{},
		shardsAmount: config.SharedWordsAggregatorAmount,
	}, nil
}

func (w *SharedWordsJoinerWorker) Run() {
	done := make(chan struct{})
	w.handleSignals(done)
	defer close(done)
	var wg sync.WaitGroup

	utils.SpawnWithWG(&wg, func() {
		w.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			w.handleMessage(msg, ack, nack)
		})
	})

	wg.Wait()
	w.shutdown()
}

func (w *SharedWordsJoinerWorker) handleSignals(done chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-signals:
			slog.Info("Graceful shutdown started...")
			w.inputQueue.StopConsuming()
		case <-done:
			return
		}
	}()
}

func (w *SharedWordsJoinerWorker) shutdown() {
	if err := w.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := w.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
	if err := w.q3EofOutput.Close(); err != nil {
		slog.Error("While closing Q3 EOF output queue", "err", err)
	}
}

func (w *SharedWordsJoinerWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.SharedWordsPartialCode:
		if err := w.handlePartial(msg); err != nil {
			slog.Error("While handling Shared Words Partial message", "err", err)
			nack()
			return
		}
	case messages.EOFCode:
		if err := w.handleEOF(msg); err != nil {
			slog.Error("While handling EOF message", "err", err)
			nack()
			return
		}
	default:
		slog.Debug("Received unexpected inner message code")
		nack()
		return
	}
	ack()
}

func (w *SharedWordsJoinerWorker) handlePartial(msg middleware.Message) error {
	partial, err := messages.SharedWordsPartialFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Debug("While reading Shared Words Partial message")
		return err
	}
	utils.WithLock(&w.lock, func() {
		state, exists := w.clientStates[msg.ClientID]
		if !exists {
			state = &ClientState{words: map[string]struct{}{}}
			w.clientStates[msg.ClientID] = state
		}
		for word := range partial.Words {
			state.words[word] = struct{}{}
		}
	})
	return nil
}

func (w *SharedWordsJoinerWorker) handleEOF(msg middleware.Message) error {
	slog.Info("Received EOF message")

	shouldFinish := false
	utils.WithLock(&w.lock, func() {
		state, exists := w.clientStates[msg.ClientID]
		if !exists {
			state = &ClientState{words: map[string]struct{}{}}
			w.clientStates[msg.ClientID] = state
		}
		state.eofCount++
		if state.eofCount >= w.shardsAmount {
			shouldFinish = true
		}
	})

	if shouldFinish {
		return w.finishClient(msg.ClientID)
	}
	return nil
}

func (w *SharedWordsJoinerWorker) finishClient(clientID uint64) error {
	words := w.takeWords(clientID)

	wordList := make([]string, 0, len(words))
	for word := range words {
		wordList = append(wordList, word)
	}
	sort.Strings(wordList)

	batch := qresults.NewEmptySharedWordBatch(len(wordList))
	for _, word := range wordList {
		batch.Add(&qresults.SharedWord{Word: word})
	}
	q3Result := qresults.Q3Result{ResultWords: batch}

	if err := w.publishQueryResult(clientID, qresults.Q3Code, &q3Result); err != nil {
		slog.Debug("While publishing Q3 Result message", "err", err)
		return err
	}

	eof := messages.EofMsg{}
	eofMsg, err := middleware.NewMessage(clientID, messages.EOFCode, &eof)
	if err != nil {
		slog.Debug("While creating Q3 EOF message", "err", err)
		return err
	}
	if err := w.q3EofOutput.Send(*eofMsg); err != nil {
		slog.Debug("While sending Q3 EOF message", "err", err)
		return err
	}

	utils.WithLock(&w.lock, func() {
		delete(w.clientStates, clientID)
	})
	return nil
}

func (w *SharedWordsJoinerWorker) takeWords(clientID uint64) map[string]struct{} {
	return utils.WithLockAndReturn(&w.lock, func() map[string]struct{} {
		state, exists := w.clientStates[clientID]
		if !exists {
			return map[string]struct{}{}
		}
		return state.words
	})
}

// publishQueryResult envuelve el payload en un QueryResult antes de publicarlo en results_queue.
func (w *SharedWordsJoinerWorker) publishQueryResult(clientID uint64, queryCode qresults.QueryResultCode, payload generic.SerializableMessage) error {
	payloadBytes, err := payload.ToBytes()
	if err != nil {
		return err
	}
	queryResult := qresults.QueryResult{
		QueryCode:     queryCode,
		ResultPayload: payloadBytes,
	}
	msg, err := middleware.NewMessage(clientID, messages.ResultCode, &queryResult)
	if err != nil {
		slog.Debug("While creating Query Result message", "err", err)
		return err
	}
	if err := w.outputQueue.Send(*msg); err != nil {
		slog.Debug("While sending Query Result message", "err", err)
		return err
	}
	return nil
}
