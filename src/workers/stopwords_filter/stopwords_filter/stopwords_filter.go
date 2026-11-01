package stopwordsfilter

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"

	"github.com/bbalet/stopwords"
)

type StopwordsFilterConfig struct {
	MomHost       string
	MomPort       int
	InputQueue    string
	OutputQueueQ3 string
	OutputQueueQ4 string
	OutputQueueQ5 string
}

type StopwordsFilterWorker struct {
	inputQueue    middleware.Middleware
	outputQueueQ3 middleware.Middleware
	outputQueueQ4 middleware.Middleware
	outputQueueQ5 middleware.Middleware
}

func New(config StopwordsFilterConfig) (*StopwordsFilterWorker, error) {
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
	return &StopwordsFilterWorker{
		inputQueue:    inputQueue,
		outputQueueQ3: outputQueueQ3,
		outputQueueQ4: outputQueueQ4,
		outputQueueQ5: outputQueueQ5,
	}, nil
}

func (w *StopwordsFilterWorker) Run() {
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

func (w *StopwordsFilterWorker) handleSignals(done chan struct{}) {
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

func (w *StopwordsFilterWorker) shutdown() {
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

func (w *StopwordsFilterWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.VocabularyCode:
		if err := w.handleVocabularyMsg(msg); err != nil {
			slog.Error("While handling Extracted Vocabulary Batch message")
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

func (w *StopwordsFilterWorker) handleVocabularyMsg(msg middleware.Message) error {
	vocabularies, err := messages.ExtractedVocabularyBatchFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Debug("While reading Extracted Vocabulary Batch message")
		return err
	}
	cleanVocabularies(vocabularies)

	// [REEMPLAZAR CUANDO ESTEN LAS QUERYS 3 Y 4]
	// _, _, wordCountBatch := filterByQuery(*vocabularies)
	wordCounts := filterByQuery(*vocabularies)

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

func (w *StopwordsFilterWorker) handleEOF(msg middleware.Message) error {
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

func cleanVocabularies(vocabularies *generic.Batch[*messages.ExtractedVocabulary]) {
	for _, vocab := range vocabularies.Items {
		cleanVocabulary(vocab)
	}
}

func cleanVocabulary(vocabulary *messages.ExtractedVocabulary) {
	newVocab := make(map[string]struct{})
	for word := range vocabulary.Vocabulary {
		cleanWord := stopwords.CleanString(word, vocabulary.InLanguage, false)
		if len(cleanWord) > 0 && cleanWord != " " {
			newVocab[cleanWord] = struct{}{}
		}
	}
	vocabulary.Vocabulary = newVocab
}

// [REEMPLAZAR CUANDO ESTEN LAS QUERYS 3 Y 4]
// func filterByQuery(vocabularies generic.Batch[*messages.ExtractedVocabulary]) (generic.Batch[*messages.SharedWordCandidate], generic.Batch[*messages.PartialTopCandidate], generic.Batch[*messages.WordCount]) {
func filterByQuery(vocabularies generic.Batch[*messages.ExtractedVocabulary]) generic.Batch[*messages.WordCount] {
	// sharedWordsCandidates :=
	// partialTopCandidates :=
	wordCounts := messages.NewEmptyWordCountBatch(vocabularies.Len())

	for _, v := range vocabularies.Items {
		wordCount := uint32(len(v.Vocabulary))
		wordCounts.Add(&messages.WordCount{
			ArticleID:    v.ArticleID,
			DateModified: v.DateModified,
			URL:          v.URL,
			WordCount:    wordCount,
		})
	}
	return wordCounts
}
