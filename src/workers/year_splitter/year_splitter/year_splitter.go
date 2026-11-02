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
	MomHost        string
	MomPort        int
	InputQueue     string
	OutputQueueOld string
	OutputQueueNew string
}

type YearSplitterWorker struct {
	inputQueue     middleware.Middleware
	outputQueueOld middleware.Middleware
	outputQueueNew middleware.Middleware
}

func New(config YearSplitterConfig) (*YearSplitterWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	outputQueueOld, err := middleware.CreateQueueMiddleware(config.OutputQueueOld, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}
	outputQueueNew, err := middleware.CreateQueueMiddleware(config.OutputQueueNew, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueueOld.Close()
		return nil, err
	}
	return &YearSplitterWorker{
		inputQueue:     inputQueue,
		outputQueueOld: outputQueueOld,
		outputQueueNew: outputQueueNew,
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
	if err := w.outputQueueOld.Close(); err != nil {
		slog.Error("While closing Old output queue", "err", err)
	}
	if err := w.outputQueueNew.Close(); err != nil {
		slog.Error("While closing New output queue", "err", err)
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
	if err := w.outputQueueOld.Send(*oldArticlesMsg); err != nil {
		slog.Debug("While sending Old Articles Word Count Batch message", "err", err)
		return err
	}

	newArticlesMsg, err := middleware.NewMessage(msg.ClientID, messages.NewArticleCountCode, &newArticlesCount)
	if err != nil {
		slog.Debug("While creating New Articles Word Count Batch message", "err", err)
		return err
	}
	if err := w.outputQueueNew.Send(*newArticlesMsg); err != nil {
		slog.Debug("While sending New Articles Word Count Batch message", "err", err)
		return err
	}

	return nil
}

func (w *YearSplitterWorker) handleEOF(msg middleware.Message) error {
	eof := messages.EofMsg{}
	eofMessage, err := middleware.NewMessage(msg.ClientID, messages.EOFCode, &eof)
	if err != nil {
		slog.Debug("While creating EOF message", "err", err)
		return err
	}
	if err := w.outputQueueOld.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	if err := w.outputQueueNew.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func splitByYear(wordCountBatch generic.Batch[*messages.WordCount]) (generic.Batch[*messages.OldArticleWordCount], generic.Batch[*messages.NewArticleWordCount]) {
	oldArticlesWordCount := messages.NewEmptyOldArticleWordCountBatch(0)
	newArticlesWordCount := messages.NewEmptyNewArticleWordCountBatch(0)

	for _, a := range wordCountBatch.Items {
		year := time.UnixMilli(a.DateModified).Year()
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
