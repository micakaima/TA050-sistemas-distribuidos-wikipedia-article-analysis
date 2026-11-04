package sharedwordsaggregator

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	qresults "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type SharedWordsAggregatorConfig struct {
	Id                          int
	MomHost                     string
	MomPort                     int
	SharedWordsAggregatorPrefix string
	OutputQueue                 string
	VocabularyExtractorAmount   int
}

type SharedWordsAggregatorWorker struct {
	inputExchange             middleware.Middleware
	outputQueue               middleware.Middleware
	clientStates              map[uint64]*ClientState
	lock                      sync.Mutex
	recvEofCountMap           map[uint64]int
	vocabularyExtractorAmount int
}

// ClientState guarda, por palabra, el set de idiomas (en/fr) en los que fue vista.
type ClientState struct {
	words map[string]map[string]struct{}
}

func New(config SharedWordsAggregatorConfig) (*SharedWordsAggregatorWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchangeRoutingKey := []string{utils.GenerateRoutingKey(config.SharedWordsAggregatorPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.SharedWordsAggregatorPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	return &SharedWordsAggregatorWorker{
		inputExchange:             inputExchange,
		outputQueue:               outputQueue,
		clientStates:              map[uint64]*ClientState{},
		lock:                      sync.Mutex{},
		recvEofCountMap:           map[uint64]int{},
		vocabularyExtractorAmount: config.VocabularyExtractorAmount,
	}, nil
}

func (w *SharedWordsAggregatorWorker) Run() {
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

func (w *SharedWordsAggregatorWorker) handleSignals(done chan struct{}) {
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

func (w *SharedWordsAggregatorWorker) shutdown() {
	if err := w.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
	if err := w.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}

func (w *SharedWordsAggregatorWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.SharedWordCandidateCode:
		if err := w.handleCandidates(msg); err != nil {
			slog.Error("While handling Shared Word Candidate Batch message")
			nack()
			return
		}
	case messages.EOFCode:
		shouldFinish := utils.WithLockAndReturn(&w.lock, func() bool {
			w.recvEofCountMap[msg.ClientID]++
			return w.recvEofCountMap[msg.ClientID] >= w.vocabularyExtractorAmount
		})
		if !shouldFinish {
			ack()
			return
		}
		utils.WithLock(&w.lock, func() {
			delete(w.recvEofCountMap, msg.ClientID)
		})
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

func (w *SharedWordsAggregatorWorker) handleCandidates(msg middleware.Message) error {
	candidates, err := messages.SharedWordCandidateBatchFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Debug("While reading Shared Word Candidate Batch message")
		return err
	}
	for _, candidate := range candidates.Items {
		w.saveCandidate(msg.ClientID, *candidate)
	}
	return nil
}

func (w *SharedWordsAggregatorWorker) saveCandidate(clientID uint64, candidate messages.SharedWordCandidate) {
	if candidate.InLanguage != "en" && candidate.InLanguage != "fr" {
		return
	}
	utils.WithLock(&w.lock, func() {
		state, exists := w.clientStates[clientID]
		if !exists {
			state = &ClientState{words: map[string]map[string]struct{}{}}
			w.clientStates[clientID] = state
		}
		langs, exists := state.words[candidate.Word]
		if !exists {
			langs = map[string]struct{}{}
			state.words[candidate.Word] = langs
		}
		langs[candidate.InLanguage] = struct{}{}
	})
}

func (w *SharedWordsAggregatorWorker) handleEOF(msg middleware.Message) error {
	slog.Info("Received EOF message")

	partialWords := w.buildPartial(msg.ClientID)
	const batchSize = 1000
	wordsBatch := qresults.NewEmptySharedWordBatch(batchSize)
	for word := range partialWords {
		wordsBatch.Add(&qresults.SharedWord{Word: word})
		if wordsBatch.Len() >= batchSize {
			result := qresults.Q3Result{ResultWords: wordsBatch}
			resultMsg, err := middleware.NewMessage(msg.ClientID, messages.Q3ResultCode, &result)
			if err != nil {
				slog.Debug("While creating Q3 result message", "err", err)
				return err
			}
			if err := w.outputQueue.Send(*resultMsg); err != nil {
				slog.Debug("While sending Q3 result message", "err", err)
				return err
			}
			wordsBatch = qresults.NewEmptySharedWordBatch(batchSize)
		}
	}
	if wordsBatch.Len() > 0 || len(partialWords) == 0 {
		result := qresults.Q3Result{ResultWords: wordsBatch}
		resultMsg, err := middleware.NewMessage(msg.ClientID, messages.Q3ResultCode, &result)
		if err != nil {
			slog.Debug("While creating Q3 result message", "err", err)
			return err
		}
		if err := w.outputQueue.Send(*resultMsg); err != nil {
			slog.Debug("While sending Q3 result message", "err", err)
			return err
		}
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

// buildPartial devuelve las palabras que fueron vistas en ambos idiomas (en y fr).
func (w *SharedWordsAggregatorWorker) buildPartial(clientID uint64) map[string]struct{} {
	return utils.WithLockAndReturn(&w.lock, func() map[string]struct{} {
		result := map[string]struct{}{}
		state, exists := w.clientStates[clientID]
		if !exists {
			return result
		}
		for word, langs := range state.words {
			_, hasEn := langs["en"]
			_, hasFr := langs["fr"]
			if hasEn && hasFr {
				result[word] = struct{}{}
			}
		}
		return result
	})
}
