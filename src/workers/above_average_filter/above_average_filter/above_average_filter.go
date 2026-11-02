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
	MomHost           string
	MomPort           int
	DataInputQueue    string
	AverageInputQueue string
	OutputQueue       string
}

type AboveAverageFilterWorker struct {
	dataInputQueue    middleware.Middleware
	averageInputQueue middleware.Middleware
	outputQueue       middleware.Middleware
	clientStates      map[uint64]*ClientState
	lock              sync.Mutex
}

type ClientState struct {
	articlesWordCounts []messages.NewArticleWordCount
	average            float64
	receivedAverage    bool
	receivedEof        bool
}

func New(config AboveAverageFilterConfig) (*AboveAverageFilterWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	dataInputQueue, err := middleware.CreateQueueMiddleware(config.DataInputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	averageInputQueue, err := middleware.CreateQueueMiddleware(config.AverageInputQueue, connSettings)
	if err != nil {
		dataInputQueue.Close()
		return nil, err
	}
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		dataInputQueue.Close()
		averageInputQueue.Close()
		return nil, err
	}
	return &AboveAverageFilterWorker{
		dataInputQueue:    dataInputQueue,
		averageInputQueue: averageInputQueue,
		outputQueue:       outputQueue,
		clientStates:      map[uint64]*ClientState{},
		lock:              sync.Mutex{},
	}, nil
}

func (w *AboveAverageFilterWorker) Run() {
	done := make(chan struct{})
	w.handleSignals(done)
	defer close(done)
	var wg sync.WaitGroup

	utils.SpawnWithWG(&wg, func() {
		w.dataInputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			w.handleDataMessage(msg, ack, nack)
		})
	})
	utils.SpawnWithWG(&wg, func() {
		w.averageInputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
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
			w.dataInputQueue.StopConsuming()
			w.averageInputQueue.StopConsuming()
		case <-done:
			return
		}
	}()
}

func (w *AboveAverageFilterWorker) shutdown() {
	if err := w.dataInputQueue.Close(); err != nil {
		slog.Error("While closing data input queue", "err", err)
	}
	if err := w.averageInputQueue.Close(); err != nil {
		slog.Error("While closing average input queue", "err", err)
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
	utils.WithLock(&w.lock, func() {
		for _, wc := range wordsCount.Items {
			w.saveWordCount(msg.ClientID, *wc)
		}
	})
	return nil
}

func (w *AboveAverageFilterWorker) handleEOF(msg middleware.Message) error {
	slog.Info("Received EOF message")
	return utils.WithLockAndReturn(&w.lock, func() error {
		if _, exists := w.clientStates[msg.ClientID]; !exists {
			w.clientStates[msg.ClientID] = &ClientState{}
		}
		w.clientStates[msg.ClientID].receivedEof = true
		if w.clientStates[msg.ClientID].receivedAverage {
			return w.sendQuery5Result(msg.ClientID)
		}
		return nil
	})
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

	utils.WithLock(&w.lock, func() {
		if _, exists := w.clientStates[msg.ClientID]; !exists {
			w.clientStates[msg.ClientID] = &ClientState{}
		}
		w.clientStates[msg.ClientID].average = oldAverage.Average
		w.clientStates[msg.ClientID].receivedAverage = true
		if w.clientStates[msg.ClientID].receivedEof {
			if err := w.sendQuery5Result(msg.ClientID); err != nil {
				slog.Error("While sending Q5 Result message", "err", err)
				nack()
				return
			}
		}
		ack()
	})
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

	q5Result := qresult.Q5Result{ResultArticles: aboveAverageArticles}
	q5ResultMsg, err := middleware.NewMessage(clientID, messages.ResultCode, &q5Result)
	if err != nil {
		slog.Debug("While creating Q5 Result message", "err", err)
		return err
	}
	if err := w.outputQueue.Send(*q5ResultMsg); err != nil {
		slog.Debug("While sending Q5 Result message", "err", err)
		return err
	}

	utils.WithLock(&w.lock, func() {
		delete(w.clientStates, clientID)
	})
	return nil
}
