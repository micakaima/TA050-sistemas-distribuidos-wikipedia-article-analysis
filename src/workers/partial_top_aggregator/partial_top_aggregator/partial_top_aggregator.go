package partialtopaggregator

import (
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"

	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	qresults "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

const topWordsAmount = 20

type PartialTopAggregatorConfig struct {
	Id                         int
	MomHost                    string
	MomPort                    int
	PartialTopAggregatorPrefix string
	OutputQueue                string
}

type PartialTopAggregatorWorker struct {
	inputExchange middleware.Middleware
	outputQueue   middleware.Middleware
	clientStates  map[uint64]map[string]uint32
	lock          sync.Mutex
}

func New(config PartialTopAggregatorConfig) (*PartialTopAggregatorWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchangeRoutingKey := []string{utils.GenerateRoutingKey(config.PartialTopAggregatorPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.PartialTopAggregatorPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	return &PartialTopAggregatorWorker{
		inputExchange: inputExchange,
		outputQueue:   outputQueue,
		clientStates:  map[uint64]map[string]uint32{},
		lock:          sync.Mutex{},
	}, nil
}

func (w *PartialTopAggregatorWorker) Run() {
	done := make(chan struct{})
	w.handleSignals(done)
	defer close(done)
	var wg sync.WaitGroup

	utils.SpawnWithWG(&wg, func() {
		w.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			w.handleMessage(msg, ack, nack)
		})
	})

	wg.Wait()
	w.shutdown()
}

func (w *PartialTopAggregatorWorker) handleSignals(done chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-signals:
			slog.Info("Graceful shutdown started...")
			w.inputExchange.StopConsuming()
		case <-done:
			return
		}
	}()
}

func (w *PartialTopAggregatorWorker) shutdown() {
	if err := w.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
	if err := w.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}

func (w *PartialTopAggregatorWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.PartialTopCandidateCode:
		if err := w.handleCandidates(msg); err != nil {
			slog.Error("While handling Partial Top Candidate Batch message")
			nack()
			return
		}
	case messages.EOFCode:
		if err := w.handleEOF(msg); err != nil {
			slog.Error("While handling EOF message")
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

func (w *PartialTopAggregatorWorker) handleCandidates(msg middleware.Message) error {
	candidates, err := messages.PartialTopCandidateBatchFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Debug("While reading Partial Top Candidate Batch message")
		return err
	}
	for _, candidate := range candidates.Items {
		w.saveCandidate(msg.ClientID, *candidate)
	}
	return nil
}

func (w *PartialTopAggregatorWorker) saveCandidate(clientID uint64, candidate messages.PartialTopCandidate) {
	utils.WithLock(&w.lock, func() {
		counts, exists := w.clientStates[clientID]
		if !exists {
			counts = map[string]uint32{}
			w.clientStates[clientID] = counts
		}
		counts[candidate.Word]++
	})
}

func (w *PartialTopAggregatorWorker) handleEOF(msg middleware.Message) error {
	slog.Info("Received EOF message")

	topWords := w.buildTopWords(msg.ClientID)
	wordsBatch := qresults.NewEmptyWordFrequencyBatch(len(topWords))
	for _, word := range topWords {
		wordsBatch.Add(word)
	}

	result := qresults.Q4Result{TopWords: wordsBatch}
	resultMsg, err := middleware.NewMessage(msg.ClientID, messages.TopWordsPartialCode, &result)
	if err != nil {
		slog.Debug("While creating Q4 partial result message", "err", err)
		return err
	}
	if err := w.outputQueue.Send(*resultMsg); err != nil {
		slog.Debug("While sending Q4 partial result message", "err", err)
		return err
	}

	eof := messages.EofMsg{}
	eofMsg, err := middleware.NewMessage(msg.ClientID, messages.EOFCode, &eof)
	if err != nil {
		slog.Debug("While creating EOF message", "err", err)
		return err
	}
	if err := w.outputQueue.Send(*eofMsg); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	utils.WithLock(&w.lock, func() {
		delete(w.clientStates, msg.ClientID)
	})
	return nil
}

// buildTopWords devuelve las top 20 palabras (count desc, word asc) de la partición del cliente.
func (w *PartialTopAggregatorWorker) buildTopWords(clientID uint64) []*qresults.WordFrequency {
	counts := utils.WithLockAndReturn(&w.lock, func() map[string]uint32 {
		result := map[string]uint32{}
		state, exists := w.clientStates[clientID]
		if !exists {
			return result
		}
		for word, count := range state {
			result[word] = count
		}
		return result
	})

	words := make([]*qresults.WordFrequency, 0, len(counts))
	for word, count := range counts {
		words = append(words, &qresults.WordFrequency{Word: word, Count: count})
	}
	sort.Slice(words, func(i, j int) bool {
		if words[i].Count != words[j].Count {
			return words[i].Count > words[j].Count // count desc
		}
		return words[i].Word < words[j].Word // word asc
	})
	if len(words) > topWordsAmount {
		words = words[:topWordsAmount]
	}
	return words
}
