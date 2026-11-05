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
	Id                        int
	MomHost                   string
	MomPort                   int
	OldArticlesAveragerPrefix string
	AboveAverageFilterAmount  int
	AboveAverageFilterPrefix  string
}

type OldArticlesAveragerWorker struct {
	inputExchange            middleware.Middleware
	outputExchange           middleware.ExchangeMiddleware
	clientStates             map[uint64]*ClientState
	aboveAverageFilterAmount int
	aboveAverageFilterPrefix string
}

type ClientState struct {
	totalWords uint32
	totalCount uint32
}

func New(config OldArticlesAveragerConfig) (*OldArticlesAveragerWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchangeRoutingKey := []string{utils.GenerateRoutingKey(config.OldArticlesAveragerPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.OldArticlesAveragerPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AboveAverageFilterPrefix, []string{}, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	return &OldArticlesAveragerWorker{
		inputExchange:            inputExchange,
		outputExchange:           outputExchange,
		clientStates:             map[uint64]*ClientState{},
		aboveAverageFilterAmount: config.AboveAverageFilterAmount,
		aboveAverageFilterPrefix: config.AboveAverageFilterPrefix,
	}, nil
}

func (w *OldArticlesAveragerWorker) Run() {
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

func (w *OldArticlesAveragerWorker) handleSignals(done chan struct{}) {
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

func (w *OldArticlesAveragerWorker) shutdown() {
	if err := w.inputExchange.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := w.outputExchange.Close(); err != nil {
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
	if err := w.sendToAboveAverageFilter(msg.ClientID, *averageMsg); err != nil {
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

func (w *OldArticlesAveragerWorker) sendToAboveAverageFilter(clientID uint64, msg middleware.Message) error {
	targetNode := utils.GetTargetNode(clientID, w.aboveAverageFilterAmount)
	routingKey := utils.GenerateRoutingKey(w.aboveAverageFilterPrefix, targetNode)
	if err := w.outputExchange.SendToKey(msg, routingKey); err != nil {
		slog.Debug("While sending New Articles Word Count Batch message", "err", err)
		return err
	}
	return nil
}
