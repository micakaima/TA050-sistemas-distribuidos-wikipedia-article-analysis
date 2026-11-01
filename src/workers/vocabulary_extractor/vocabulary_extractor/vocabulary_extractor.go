package vocabularyextractor

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

	"github.com/bbalet/stopwords"
)

type VocabularyExtractorConfig struct {
	MomHost       string
	MomPort       int
	InputQueue    string
	OutputQueueQ3 string
	OutputQueueQ4 string
	OutputQueueQ5 string
}

type VocabularyExtractorWorker struct {
	inputQueue    middleware.Middleware
	outputQueueQ3 middleware.Middleware
	outputQueueQ4 middleware.Middleware
	outputQueueQ5 middleware.Middleware
}

func New(config VocabularyExtractorConfig) (*VocabularyExtractorWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	outputQueueQ3, err := middleware.CreateQueueMiddleware(config.OutputQueueQ3, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}
	outputQueueQ4, err := middleware.CreateQueueMiddleware(config.OutputQueueQ4, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueueQ3.Close()
		return nil, err
	}
	outputQueueQ5, err := middleware.CreateQueueMiddleware(config.OutputQueueQ5, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQueueQ3.Close()
		outputQueueQ4.Close()
		return nil, err
	}
	return &VocabularyExtractorWorker{
		inputQueue:    inputQueue,
		outputQueueQ3: outputQueueQ3,
		outputQueueQ4: outputQueueQ4,
		outputQueueQ5: outputQueueQ5,
	}, nil
}

func (w *VocabularyExtractorWorker) Run() {
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

func (w *VocabularyExtractorWorker) handleSignals(done chan struct{}) {
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

func (w *VocabularyExtractorWorker) shutdown() {
	if err := w.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := w.outputQueueQ3.Close(); err != nil {
		slog.Error("While closing Q3 output queue", "err", err)
	}
	if err := w.outputQueueQ4.Close(); err != nil {
		slog.Error("While closing Q4 output queue", "err", err)
	}
	if err := w.outputQueueQ5.Close(); err != nil {
		slog.Error("While closing Q5 output queue", "err", err)
	}
}

func (w *VocabularyExtractorWorker) handleMessage(msg middleware.Message, ack, nack func()) {
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

func (w *VocabularyExtractorWorker) handleRawAbstract(msg middleware.Message) error {
	rawAbstractBatch, err := messages.RawAbstractBatchFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Debug("While reading Raw Abstracts Batch message")
		return err
	}
	cleanVocabularies := extractCleanVocabularies(*rawAbstractBatch)
	wordCounts := filterByQuery(cleanVocabularies)

	// AGREGAR EL RESTO DE MENSAJES CUANDO ESTEN Q3 y Q4
	wordCountsMsg, err := middleware.NewMessage(msg.ClientID, messages.WordCountCode, &wordCounts)
	if err != nil {
		slog.Debug("While creating Word Count Batch message", "err", err)
		return err
	}
	if err := w.outputQueueQ5.Send(*wordCountsMsg); err != nil {
		slog.Debug("While sending Word Count Batch message", "err", err)
		return err
	}

	return nil
}

func (w *VocabularyExtractorWorker) handleEOF(msg middleware.Message) error {
	eof := messages.EofMsg{}
	eofMessage, err := middleware.NewMessage(msg.ClientID, messages.EOFCode, &eof)
	if err != nil {
		slog.Debug("While creating EOF message", "err", err)
		return err
	}
	if err := w.outputQueueQ3.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	if err := w.outputQueueQ4.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	if err := w.outputQueueQ5.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func extractCleanVocabularies(rawAbstractBatch generic.Batch[*messages.RawAbstract]) generic.Batch[*messages.ExtractedVocabulary] {
	vocabularies := messages.NewEmptyExtractedVocabularyBatch(rawAbstractBatch.Len())
	for _, abs := range rawAbstractBatch.Items {
		absVocabulary := extractCleanVocabulary(abs.Abstract, abs.InLanguage)
		cleanWordCount := len(absVocabulary)
		vocabularies.Add(&messages.ExtractedVocabulary{
			ArticleID:    abs.ArticleID,
			DateModified: abs.DateModified,
			InLanguage:   abs.InLanguage,
			Vocabulary:   absVocabulary,
			WordCount:    uint32(cleanWordCount),
		})
	}
	return vocabularies
}

func extractCleanVocabulary(abstract string, lang string) map[string]struct{} {
	cleanVocabulary := make(map[string]struct{})
	for _, word := range strings.Fields(abstract) {
		lowercaseWord := strings.ToLower(word)
		cleanWord := stopwords.CleanString(lowercaseWord, lang, false)
		if _, exists := cleanVocabulary[cleanWord]; !exists && len(cleanWord) > 0 && cleanWord != " " {
			cleanVocabulary[cleanWord] = struct{}{}
		}
	}
	return cleanVocabulary
}

// AGREGAR EL RESTO DE RESULTADOS CUANDO ESTEN Q3 y Q4
func filterByQuery(cleanVocabularies generic.Batch[*messages.ExtractedVocabulary]) generic.Batch[*messages.WordCount] {
	wordCounts := messages.NewEmptyWordCountBatch(cleanVocabularies.Len())

	for _, v := range cleanVocabularies.Items {
		wordCounts.Add(&messages.WordCount{
			ArticleID:    v.ArticleID,
			DateModified: v.DateModified,
			URL:          v.URL,
			WordCount:    v.WordCount,
		})
	}
	return wordCounts
}
