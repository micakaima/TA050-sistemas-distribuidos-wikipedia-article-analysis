package column_filter

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	article "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/raw_article"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/record"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type Config struct {
	MomHost      string
	MomPort      int
	InputQueue   string
	OutputQ1ToQ2 string
	OutputQ3ToQ5 string
}

type ColumnFilterWorker struct {
	inputQueue   middleware.Middleware
	outputQ1ToQ2 middleware.Middleware
	outputQ3ToQ5 middleware.Middleware
}

func New(config Config) (*ColumnFilterWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	outputQ1ToQ2, err := middleware.CreateQueueMiddleware(config.OutputQ1ToQ2, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}
	outputQ3ToQ5, err := middleware.CreateQueueMiddleware(config.OutputQ3ToQ5, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQ1ToQ2.Close()
		return nil, err
	}
	return &ColumnFilterWorker{
		inputQueue:   inputQueue,
		outputQ1ToQ2: outputQ1ToQ2,
		outputQ3ToQ5: outputQ3ToQ5,
	}, nil
}

func (w *ColumnFilterWorker) Run() {
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

func (w *ColumnFilterWorker) handleSignals(done chan struct{}) {
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

func (w *ColumnFilterWorker) shutdown() {
	if err := w.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := w.outputQ1ToQ2.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
	if err := w.outputQ3ToQ5.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}

func (w *ColumnFilterWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.ArticlesBatchCode:
		if err := w.handleArticlesBatch(msg); err != nil {
			slog.Error("While handling Articles Batch message")
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

func (w *ColumnFilterWorker) handleArticlesBatch(msg middleware.Message) error {
	rawArticlesBatch, err := article.RawArticlesBatchFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Debug("While reading Raw Articles Batch message")
		return err
	}
	records := article.ArticlesBatchToRecords(*rawArticlesBatch)
	recordsQ1ToQ2, recordsQ3ToQ5 := filterByQuery(records)

	// Envio de batch de mensajes YearCandidate, para resolver querys Q1-Q2
	q1q2Message, err := middleware.NewMessage(msg.ClientID, messages.YearCandidateCode, &recordsQ1ToQ2)
	if err != nil {
		slog.Debug("While creating Year Candidate Batch message", "err", err)
		return err
	}
	if err := w.outputQ1ToQ2.Send(*q1q2Message); err != nil {
		slog.Debug("While sending Year Candidate Batch message", "err", err)
		return err
	}

	// Envio de batch de mensajes RawAbstract, para resolver querys Q3-Q5
	q3q5Message, err := middleware.NewMessage(msg.ClientID, messages.RawAbstractCode, &recordsQ3ToQ5)
	if err != nil {
		slog.Debug("While creating Raw Abstract Batch message", "err", err)
		return err
	}
	if err := w.outputQ3ToQ5.Send(*q3q5Message); err != nil {
		slog.Debug("While sending Raw Abstract Batch message", "err", err)
		return err
	}

	return nil
}

func (w *ColumnFilterWorker) handleEOF(msg middleware.Message) error {
	slog.Info("Received EOF message")
	eof := messages.EofMsg{}
	eofMessage, err := middleware.NewMessage(msg.ClientID, messages.EOFCode, &eof)
	if err != nil {
		slog.Debug("While creating EOF message", "err", err)
		return err
	}
	if err := w.outputQ1ToQ2.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	if err := w.outputQ3ToQ5.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func filterByQuery(records []record.Record) (generic.Batch[*messages.YearCandidate], generic.Batch[*messages.RawAbstract]) {
	q1Toq2 := messages.NewEmptyYearCandidateBatch(len(records))
	q3Toq5 := messages.NewEmptyRawAbstractBatch(len(records))

	for _, r := range records {
		q1Toq2.Add(&messages.YearCandidate{
			ArticleID:    r.ArticleID,
			DateModified: r.DateModified,
			ImageURL:     r.Image.ContentURL,
			InLanguage:   r.InLanguage.Identifier,
			Name:         r.Name,
			Sections:     r.Sections,
			URL:          r.URL,
		})
		q3Toq5.Add(&messages.RawAbstract{
			ArticleID:    r.ArticleID,
			DateModified: r.DateModified,
			InLanguage:   r.InLanguage.Identifier,
			URL:          r.URL,
			Abstract:     r.Abstract,
		})
	}
	return q1Toq2, q3Toq5
}
