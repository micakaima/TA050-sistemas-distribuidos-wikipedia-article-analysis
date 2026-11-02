package oldarticlesaverager

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type OldArticlesAveragerConfig struct {
	MomHost     string
	MomPort     int
	InputQueue  string
	OutputQueue string
}

type OldArticlesAveragerWorker struct {
	inputQueue   middleware.Middleware
	outputQueue  middleware.Middleware
	clientStates map[uint64]*ClientState
}

type ClientState struct {
	totalWords uint32
	totalCount uint32
}

func New(config OldArticlesAveragerConfig) (*OldArticlesAveragerWorker, error) {
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
	return &OldArticlesAveragerWorker{
		inputQueue:   inputQueue,
		outputQueue:  outputQueue,
		clientStates: map[uint64]*ClientState{},
	}, nil
}

func (w *OldArticlesAveragerWorker) Run() {
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

func (w *OldArticlesAveragerWorker) handleSignals(done chan struct{}) {
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

func (w *OldArticlesAveragerWorker) shutdown() {
	if err := w.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := w.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}

func (w *OldArticlesAveragerWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.OldArticleCountCode:
		if err := w.handleWordCount(msg); err != nil {
			slog.Error("While handling Old Article Word Count Batch message")
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

func (w *OldArticlesAveragerWorker) handleWordCount(msg middleware.Message) error {
	wordCounts, err := messages.OldArticleWordCountBatchFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Debug("While reading Old Article Word Count Batch message")
		return err
	}
	for _, wc := range wordCounts.Items {
		w.saveWordCount(msg.ClientID, *wc)
	}
	return nil
}

func (w *OldArticlesAveragerWorker) handleEOF(msg middleware.Message) error {
	slog.Info("Received EOF message")
	clientAverage := w.calculateAverage(msg.ClientID)

	average := messages.OldAverage{Average: clientAverage}
	averageMsg, err := middleware.NewMessage(msg.ClientID, messages.AverageCode, &average)
	if err != nil {
		slog.Debug("While creating Old Average message", "err", err)
		return err
	}
	if err := w.outputQueue.Send(*averageMsg); err != nil {
		slog.Debug("While sending Old Average message", "err", err)
		return err
	}

	delete(w.clientStates, msg.ClientID)
	return nil
}

func (w *OldArticlesAveragerWorker) saveWordCount(clientID uint64, wordCount messages.OldArticleWordCount) {
	if _, exists := w.clientStates[clientID]; !exists {
		w.clientStates[clientID] = &ClientState{}
	}
	w.clientStates[clientID].totalWords += wordCount.WordCount
	w.clientStates[clientID].totalCount++
}

func (w *OldArticlesAveragerWorker) calculateAverage(clientID uint64) float64 {
	clientState, exists := w.clientStates[clientID]
	if !exists {
		return 0
	}
	return float64(clientState.totalWords) / float64(clientState.totalCount)
}
