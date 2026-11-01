package wordcounter

import (
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type WordCounterConfig struct {
	MomHost     string
	MomPort     int
	InputQueue  string
	OutputQueue string
}

type WordCounterWorker struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
}

func New(config WordCounterConfig) (*WordCounterWorker, error) {
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
	return &WordCounterWorker{
		inputQueue:  inputQueue,
		outputQueue: outputQueue,
	}, nil
}

func (w *WordCounterWorker) Run() {
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

func (w *WordCounterWorker) handleSignals(done chan struct{}) {
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

func (w *WordCounterWorker) shutdown() {
	if err := w.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := w.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}

func (w *WordCounterWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.RawAbstractCode:
		if err := w.handleRawAbstract(msg); err != nil {
			slog.Error("While handling Raw Abstract Batch message")
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

func (w *WordCounterWorker) handleRawAbstract(msg middleware.Message) error {
	rawAbstractBatch, err := messages.RawAbstractBatchFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Debug("While reading Raw Abstracts Batch message")
		return err
	}
	vocabularies := extractVocabularies(*rawAbstractBatch)

	vocabulariesMsg, err := middleware.NewMessage(msg.ClientID, messages.VocabularyCode, &vocabularies)
	if err != nil {
		slog.Debug("While creating Extracted Vocabulary Batch message", "err", err)
		return err
	}
	if err := w.outputQueue.Send(*vocabulariesMsg); err != nil {
		slog.Debug("While sending Extracted Vocabulary Batch message", "err", err)
		return err
	}
	return nil
}

func (w *WordCounterWorker) handleEOF(msg middleware.Message) error {
	eof := messages.EofMsg{}
	eofMessage, err := middleware.NewMessage(msg.ClientID, messages.EOFCode, &eof)
	if err != nil {
		slog.Debug("While creating EOF message", "err", err)
		return err
	}
	if err := w.outputQueue.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func extractVocabularies(rawAbstractBatch generic.Batch[*messages.RawAbstract]) generic.Batch[*messages.ExtractedVocabulary] {
	vocabularies := messages.NewEmptyExtractedVocabularyBatch(rawAbstractBatch.Len())
	for _, abs := range rawAbstractBatch.Items {
		absVocabulary := extractVocabulary(abs.Abstract)
		vocabularies.Add(&messages.ExtractedVocabulary{
			ArticleID:    abs.ArticleID,
			DateModified: abs.DateModified,
			InLanguage:   abs.InLanguage,
			Vocabulary:   absVocabulary,
		})
	}
	return vocabularies
}

func extractVocabulary(abstract string) map[string]struct{} {
	vocabulary := make(map[string]struct{})
	for _, word := range strings.Fields(abstract) {
		lowercaseWord := strings.ToLower(word)
		if _, exists := vocabulary[lowercaseWord]; !exists {
			vocabulary[lowercaseWord] = struct{}{}
		}
	}
	return vocabulary
}
