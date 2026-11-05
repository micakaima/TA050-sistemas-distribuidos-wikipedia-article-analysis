package yearsplitter

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

const SPLITTER_YEAR = 2020

type YearSplitterConfig struct {
	MomHost                   string
	MomPort                   int
	InputQueue                string
	VocabularyExtractorAmount int
	OldArticlesAveragerAmount int
	OldArticlesAveragerPrefix string
	AboveAverageFilterAmount  int
	AboveAverageFilterPrefix  string
}

type YearSplitterWorker struct {
	inputQueue                middleware.Middleware
	outputExchangeOld         middleware.ExchangeMiddleware
	outputExchangeNew         middleware.ExchangeMiddleware
	recvEofCountMap           map[uint64]int
	vocabularyExtractorAmount int
	oldArticlesAveragerAmount int
	oldArticlesAveragerPrefix string
	aboveAverageFilterAmount  int
	aboveAverageFilterPrefix  string
}

func New(config YearSplitterConfig) (*YearSplitterWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeOld, err := middleware.CreateExchangeMiddleware(config.OldArticlesAveragerPrefix, []string{}, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	outputExchangeNew, err := middleware.CreateExchangeMiddleware(config.AboveAverageFilterPrefix, []string{}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchangeOld.Close()
		return nil, err
	}

	return &YearSplitterWorker{
		inputQueue:                inputQueue,
		outputExchangeOld:         outputExchangeOld,
		outputExchangeNew:         outputExchangeNew,
		recvEofCountMap:           map[uint64]int{},
		vocabularyExtractorAmount: config.VocabularyExtractorAmount,
		oldArticlesAveragerAmount: config.OldArticlesAveragerAmount,
		oldArticlesAveragerPrefix: config.OldArticlesAveragerPrefix,
		aboveAverageFilterAmount:  config.AboveAverageFilterAmount,
		aboveAverageFilterPrefix:  config.AboveAverageFilterPrefix,
	}, nil
}

func (w *YearSplitterWorker) Run() {
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

func (w *YearSplitterWorker) handleSignals(done chan struct{}) {
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

func (w *YearSplitterWorker) shutdown() {
	if err := w.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := w.outputExchangeOld.Close(); err != nil {
		slog.Error("While closing Old output exchange", "err", err)
	}
	if err := w.outputExchangeNew.Close(); err != nil {
		slog.Error("While closing New output exchange", "err", err)
	}
}

func (w *YearSplitterWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.WordCountCode:
		if err := w.handleWordCount(msg); err != nil {
			slog.Error("While handling Word Count Batch message")
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

func (w *YearSplitterWorker) handleWordCount(msg middleware.Message) error {
	wordCountBatch, err := messages.WordCountBatchFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Debug("While reading Word Count Batch message")
		return err
	}
	oldArticlesCount, newArticlesCount := splitByYear(*wordCountBatch)

	oldArticlesMsg, err := middleware.NewMessage(msg.ClientID, messages.OldArticleCountCode, &oldArticlesCount)
	if err != nil {
		slog.Debug("While creating Old Articles Word Count Batch message", "err", err)
		return err
	}
	if err := w.sendToOldAverager(msg.ClientID, *oldArticlesMsg); err != nil {
		return err
	}

	newArticlesMsg, err := middleware.NewMessage(msg.ClientID, messages.NewArticleCountCode, &newArticlesCount)
	if err != nil {
		slog.Debug("While creating New Articles Word Count Batch message", "err", err)
		return err
	}
	if err := w.sendToAboveAverageFilter(msg.ClientID, *newArticlesMsg); err != nil {
		return err
	}

	return nil
}

func (w *YearSplitterWorker) handleEOF(msg middleware.Message) error {
	w.recvEofCountMap[msg.ClientID]++
	if w.recvEofCountMap[msg.ClientID] < w.vocabularyExtractorAmount {
		return nil
	}
	delete(w.recvEofCountMap, msg.ClientID)

	slog.Info("Received all EOF messages from VocabularyExtractor")
	eof := messages.EofMsg{}
	eofMessage, err := middleware.NewMessage(msg.ClientID, messages.EOFCode, &eof)
	if err != nil {
		slog.Debug("While creating EOF message", "err", err)
		return err
	}

	if err := w.sendToOldAverager(msg.ClientID, *eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	if err := w.sendToAboveAverageFilter(msg.ClientID, *eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func splitByYear(wordCountBatch generic.Batch[*messages.WordCount]) (generic.Batch[*messages.OldArticleWordCount], generic.Batch[*messages.NewArticleWordCount]) {
	oldArticlesWordCount := messages.NewEmptyOldArticleWordCountBatch(0)
	newArticlesWordCount := messages.NewEmptyNewArticleWordCountBatch(0)

	for _, a := range wordCountBatch.Items {
		year := time.UnixMilli(a.DateModified).UTC().Year()
		if year <= SPLITTER_YEAR {
			oldArticlesWordCount.Add(&messages.OldArticleWordCount{
				ArticleID: a.ArticleID,
				WordCount: a.WordCount,
			})
		} else {
			newArticlesWordCount.Add(&messages.NewArticleWordCount{
				ArticleID: a.ArticleID,
				URL:       a.URL,
				WordCount: a.WordCount,
			})
		}
	}
	return oldArticlesWordCount, newArticlesWordCount
}

func (w *YearSplitterWorker) sendToOldAverager(clientID uint64, msg middleware.Message) error {
	targetNode := utils.GetTargetNode(clientID, w.oldArticlesAveragerAmount)
	routingKey := utils.GenerateRoutingKey(w.oldArticlesAveragerPrefix, targetNode)
	if err := w.outputExchangeOld.SendToKey(msg, routingKey); err != nil {
		slog.Debug("While sending Old Articles Word Count Batch message", "err", err)
		return err
	}
	return nil
}

func (w *YearSplitterWorker) sendToAboveAverageFilter(clientID uint64, msg middleware.Message) error {
	targetNode := utils.GetTargetNode(clientID, w.aboveAverageFilterAmount)
	routingKey := utils.GenerateRoutingKey(w.aboveAverageFilterPrefix, targetNode)
	if err := w.outputExchangeNew.SendToKey(msg, routingKey); err != nil {
		slog.Debug("While sending New Articles Word Count Batch message", "err", err)
		return err
	}
	return nil
}
