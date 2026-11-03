package aboveaveragefilter

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
	qresult "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type AboveAverageFilterConfig struct {
	Id                              int
	MomHost                         string
	MomPort                         int
	OutputQueue                     string
	AboveAverageFilterPrefixData    string
	AboveAverageFilterPrefixAverage string
}

type AboveAverageFilterWorker struct {
	dataInputExchange    middleware.Middleware
	averageInputExchange middleware.Middleware
	outputQueue          middleware.Middleware
	clientStates         map[uint64]*ClientState
	lock                 sync.Mutex
}

type ClientState struct {
	articlesWordCounts []messages.NewArticleWordCount
	average            float64
	receivedAverage    bool
	receivedEof        bool
}

func New(config AboveAverageFilterConfig) (*AboveAverageFilterWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	dataInputExchangeRoutingKey := []string{utils.GenerateRoutingKey(config.AboveAverageFilterPrefixData, config.Id)}
	dataInputExchange, err := middleware.CreateExchangeMiddleware(config.AboveAverageFilterPrefixData, dataInputExchangeRoutingKey, connSettings)
	if err != nil {
		return nil, err
	}

	averageInputExchangeRoutingKey := []string{utils.GenerateRoutingKey(config.AboveAverageFilterPrefixAverage, config.Id)}
	averageInputExchange, err := middleware.CreateExchangeMiddleware(config.AboveAverageFilterPrefixAverage, averageInputExchangeRoutingKey, connSettings)
	if err != nil {
		dataInputExchange.Close()
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		dataInputExchange.Close()
		averageInputExchange.Close()
		return nil, err
	}
	return &AboveAverageFilterWorker{
		dataInputExchange:    dataInputExchange,
		averageInputExchange: averageInputExchange,
		outputQueue:          outputQueue,
		clientStates:         map[uint64]*ClientState{},
		lock:                 sync.Mutex{},
	}, nil
}

func (w *AboveAverageFilterWorker) Run() {
	done := make(chan struct{})
	w.handleSignals(done)
	defer close(done)
	var wg sync.WaitGroup

	utils.SpawnWithWG(&wg, func() {
		w.dataInputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			w.handleDataMessage(msg, ack, nack)
		})
	})
	utils.SpawnWithWG(&wg, func() {
		w.averageInputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			w.handleAverageMessage(msg, ack, nack)
		})
	})

	wg.Wait()
	w.shutdown()
}

func (w *AboveAverageFilterWorker) handleSignals(done chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-signals:
			slog.Info("Graceful shutdown started...")
			w.dataInputExchange.StopConsuming()
			w.averageInputExchange.StopConsuming()
		case <-done:
			return
		}
	}()
}

func (w *AboveAverageFilterWorker) shutdown() {
	if err := w.dataInputExchange.Close(); err != nil {
		slog.Error("While closing data input exchange", "err", err)
	}
	if err := w.averageInputExchange.Close(); err != nil {
		slog.Error("While closing average input exchange", "err", err)
	}
	if err := w.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}

func (w *AboveAverageFilterWorker) handleDataMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.NewArticleCountCode:
		if err := w.handleWordCount(msg); err != nil {
			slog.Error("While handling New Article Word Count Batch message", "err", err)
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

func (w *AboveAverageFilterWorker) handleWordCount(msg middleware.Message) error {
	wordsCount, err := messages.NewArticleWordCountBatchFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Debug("While reading New Article Word Count Batch message")
		return err
	}
	for _, wc := range wordsCount.Items {
		w.saveWordCount(msg.ClientID, *wc)
	}
	return nil
}

func (w *AboveAverageFilterWorker) handleEOF(msg middleware.Message) error {
	slog.Info("Received EOF message")

	canSendResult := false
	utils.WithLock(&w.lock, func() {
		if _, exists := w.clientStates[msg.ClientID]; !exists {
			w.clientStates[msg.ClientID] = &ClientState{}
		}
		state := w.clientStates[msg.ClientID]
		state.receivedEof = true
		if state.receivedAverage {
			canSendResult = true
		}
	})

	if canSendResult {
		return w.sendQuery5Result(msg.ClientID)
	}
	return nil
}

func (w *AboveAverageFilterWorker) handleAverageMessage(msg middleware.Message, ack, nack func()) {
	if msg.MsgCode != messages.AverageCode {
		slog.Debug("Received unexpected inner message code")
		nack()
		return
	}
	oldAverage, err := messages.OldAverageFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Error("While reading Old Average message")
		nack()
		return
	}

	canSendResult := false
	utils.WithLock(&w.lock, func() {
		if _, exists := w.clientStates[msg.ClientID]; !exists {
			w.clientStates[msg.ClientID] = &ClientState{}
		}
		state := w.clientStates[msg.ClientID]
		state.average = oldAverage.Average
		state.receivedAverage = true
		if state.receivedEof {
			canSendResult = true
		}
	})

	if canSendResult {
		if err := w.sendQuery5Result(msg.ClientID); err != nil {
			slog.Error("While sending Q5 Result message", "err", err)
			nack()
			return
		}
		if err := w.sendEOF(msg.ClientID); err != nil {
			slog.Error("While sending EOF message", "err", err)
			nack()
			return
		}
		utils.WithLock(&w.lock, func() {
			delete(w.clientStates, msg.ClientID)
		})
	}
	ack()
}

func (w *AboveAverageFilterWorker) saveWordCount(clientID uint64, wordCount messages.NewArticleWordCount) {
	utils.WithLock(&w.lock, func() {
		if _, exists := w.clientStates[clientID]; !exists {
			w.clientStates[clientID] = &ClientState{}
		}
		clientState := w.clientStates[clientID]
		clientState.articlesWordCounts = append(clientState.articlesWordCounts, messages.NewArticleWordCount{
			ArticleID: wordCount.ArticleID,
			URL:       wordCount.URL,
			WordCount: wordCount.WordCount,
		})
	})
}

func (w *AboveAverageFilterWorker) filterAboveAverage(clientID uint64) (generic.Batch[*qresult.AboveAverageArticle], error) {
	aboveAverage := qresult.NewEmptyAboveAverageArticleBatch(0)
	err := utils.WithLockAndReturn(&w.lock, func() error {
		clientState, exists := w.clientStates[clientID]
		if !exists {
			return fmt.Errorf(errors.UnknownClientID)
		}
		for _, wc := range clientState.articlesWordCounts {
			if float64(wc.WordCount) > clientState.average {
				aboveAverage.Add(&qresult.AboveAverageArticle{
					ArticleID: wc.ArticleID,
					URL:       wc.URL,
					WordCount: wc.WordCount,
				})
			}
		}
		return nil
	})
	return aboveAverage, err
}

func (w *AboveAverageFilterWorker) sendQuery5Result(clientID uint64) error {
	aboveAverageArticles, err := w.filterAboveAverage(clientID)
	if err != nil {
		slog.Debug("While filtering articles with above average word count", "err", err)
		return err
	}
	resultMessage, err := middleware.NewMessage(clientID, messages.Q5ResultCode, &aboveAverageArticles)
	if err != nil || w.outputQueue.Send(*resultMessage) != nil {
		return err
	}

	return nil
}

func (w *AboveAverageFilterWorker) sendEOF(clientID uint64) error {
	eof := messages.EofMsg{}
	eofMsg, err := middleware.NewMessage(clientID, messages.EOFCode, &eof)
	if err != nil {
		slog.Debug("While creating EOF message", "err", err)
		return err
	}
	if err := w.outputQueue.Send(*eofMsg); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}
