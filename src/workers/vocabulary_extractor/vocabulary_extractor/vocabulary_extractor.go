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

const PUNCTUATION_SYMBOLS = "-.,():;!?[]{}'\""

type VocabularyExtractorConfig struct {
	Id                          int
	MomHost                     string
	MomPort                     int
	VocabularyExtractorPrefix   string
	OutputQueueQ5               string
	SharedWordsAggregatorAmount int
	SharedWordsAggregatorPrefix string
	PartialTopAggregatorAmount  int
	PartialTopAggregatorPrefix  string
	ColumnFilterAmount          int
}

type VocabularyExtractorWorker struct {
	inputExchange               middleware.Middleware
	outputExchangeQ3            middleware.ExchangeMiddleware
	outputExchangeQ4            middleware.ExchangeMiddleware
	outputQueueQ5               middleware.Middleware
	recvEofCountMap             map[uint64]int
	sharedWordsAggregatorAmount int
	sharedWordsAggregatorPrefix string
	partialTopAggregatorAmount  int
	partialTopAggregatorPrefix  string
	columnFilterAmount          int
}

func New(config VocabularyExtractorConfig) (*VocabularyExtractorWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchangeRoutingKey := []string{utils.GenerateRoutingKey(config.VocabularyExtractorPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.VocabularyExtractorPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeQ3, err := middleware.CreateExchangeMiddleware(config.SharedWordsAggregatorPrefix, []string{}, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}
	outputExchangeQ4, err := middleware.CreateExchangeMiddleware(config.PartialTopAggregatorPrefix, []string{}, connSettings)
	if err != nil {
		inputExchange.Close()
		outputExchangeQ3.Close()
		return nil, err
	}
	outputQueueQ5, err := middleware.CreateQueueMiddleware(config.OutputQueueQ5, connSettings)
	if err != nil {
		inputExchange.Close()
		outputExchangeQ3.Close()
		outputExchangeQ4.Close()
		return nil, err
	}
	return &VocabularyExtractorWorker{
		inputExchange:               inputExchange,
		outputExchangeQ3:            outputExchangeQ3,
		outputExchangeQ4:            outputExchangeQ4,
		outputQueueQ5:               outputQueueQ5,
		recvEofCountMap:             map[uint64]int{},
		sharedWordsAggregatorAmount: config.SharedWordsAggregatorAmount,
		sharedWordsAggregatorPrefix: config.SharedWordsAggregatorPrefix,
		partialTopAggregatorAmount:  config.PartialTopAggregatorAmount,
		partialTopAggregatorPrefix:  config.PartialTopAggregatorPrefix,
		columnFilterAmount:          config.ColumnFilterAmount,
	}, nil
}

func (w *VocabularyExtractorWorker) Run() {
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

func (w *VocabularyExtractorWorker) handleSignals(done chan struct{}) {
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

func (w *VocabularyExtractorWorker) shutdown() {
	if err := w.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
	if err := w.outputExchangeQ3.Close(); err != nil {
		slog.Error("While closing Q3 output exchange", "err", err)
	}
	if err := w.outputExchangeQ4.Close(); err != nil {
		slog.Error("While closing Q4 output exchange", "err", err)
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

	if err := w.sendSharedWordCandidates(msg.ClientID, cleanVocabularies); err != nil {
		slog.Debug("While sending Shared Word Candidate messages", "err", err)
		return err
	}

	if err := w.sendPartialTopCandidates(msg.ClientID, cleanVocabularies); err != nil {
		slog.Debug("While sending Partial Top Candidate messages", "err", err)
		return err
	}

	if err := w.sendWordCounts(msg.ClientID, cleanVocabularies); err != nil {
		slog.Debug("While sending Word Count messages", "err", err)
		return err
	}

	return nil
}

func (w *VocabularyExtractorWorker) sendSharedWordCandidates(clientID uint64, cleanVocabularies generic.Batch[*messages.ExtractedVocabulary]) error {
	candidatesByNode := groupSharedWordCandidates(cleanVocabularies, w.sharedWordsAggregatorAmount)
	for node, candidates := range candidatesByNode {
		msg, err := middleware.NewMessage(clientID, messages.SharedWordCandidateCode, candidates)
		if err != nil {
			slog.Debug("While creating Shared Word Candidate Batch message", "err", err)
			return err
		}
		routingKey := utils.GenerateRoutingKey(w.sharedWordsAggregatorPrefix, node)
		if err := w.outputExchangeQ3.SendToKey(*msg, routingKey); err != nil {
			slog.Debug("While sending Shared Word Candidate Batch message", "err", err)
			return err
		}
	}
	return nil
}

func (w *VocabularyExtractorWorker) sendPartialTopCandidates(clientID uint64, cleanVocabularies generic.Batch[*messages.ExtractedVocabulary]) error {
	candidatesByNode := groupPartialTopCandidates(cleanVocabularies, w.partialTopAggregatorAmount)
	for node, candidates := range candidatesByNode {
		msg, err := middleware.NewMessage(clientID, messages.PartialTopCandidateCode, candidates)
		if err != nil {
			slog.Debug("While creating Partial Top Candidate Batch message", "err", err)
			return err
		}
		routingKey := utils.GenerateRoutingKey(w.partialTopAggregatorPrefix, node)
		if err := w.outputExchangeQ4.SendToKey(*msg, routingKey); err != nil {
			slog.Debug("While sending Partial Top Candidate Batch message", "err", err)
			return err
		}
	}
	return nil
}

func (w *VocabularyExtractorWorker) sendWordCounts(clientID uint64, cleanVocabularies generic.Batch[*messages.ExtractedVocabulary]) error {
	wordCounts := getWordCounts(cleanVocabularies)
	wordCountsMsg, err := middleware.NewMessage(clientID, messages.WordCountCode, &wordCounts)
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
	slog.Info("Received EOF message")

	w.recvEofCountMap[msg.ClientID]++
	if w.recvEofCountMap[msg.ClientID] < w.columnFilterAmount {
		return nil
	}
	delete(w.recvEofCountMap, msg.ClientID)

	eof := messages.EofMsg{}
	eofMessage, err := middleware.NewMessage(msg.ClientID, messages.EOFCode, &eof)
	if err != nil {
		slog.Debug("While creating EOF message", "err", err)
		return err
	}

	// Broadcast del EOF a todos los shards de Q3.
	for n := 0; n < w.sharedWordsAggregatorAmount; n++ {
		routingKey := utils.GenerateRoutingKey(w.sharedWordsAggregatorPrefix, n)
		if err := w.outputExchangeQ3.SendToKey(*eofMessage, routingKey); err != nil {
			slog.Debug("While sending EOF message to Q3 shard", "err", err)
			return err
		}
	}

	// Broadcast del EOF a todos los shards de Q4.
	for n := 0; n < w.partialTopAggregatorAmount; n++ {
		routingKey := utils.GenerateRoutingKey(w.partialTopAggregatorPrefix, n)
		if err := w.outputExchangeQ4.SendToKey(*eofMessage, routingKey); err != nil {
			slog.Debug("While sending EOF message to Q4 shard", "err", err)
			return err
		}
	}

	if err := w.outputQueueQ5.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

// groupSharedWordCandidates agrupa los candidatos de Q3 por shard según el hash de la palabra.
func groupSharedWordCandidates(cleanVocabularies generic.Batch[*messages.ExtractedVocabulary], amount int) map[int]*generic.Batch[*messages.SharedWordCandidate] {
	candidatesByNode := make(map[int]*generic.Batch[*messages.SharedWordCandidate])
	for _, v := range cleanVocabularies.Items {
		for word := range v.Vocabulary {
			targetNode := utils.GetTargetNodeByWord(word, amount)
			if _, exists := candidatesByNode[targetNode]; !exists {
				batch := messages.NewEmptySharedWordCandidateBatch(0)
				candidatesByNode[targetNode] = &batch
			}
			candidatesByNode[targetNode].Add(&messages.SharedWordCandidate{
				ArticleID:  v.ArticleID,
				InLanguage: v.InLanguage,
				Word:       word,
			})
		}
	}
	return candidatesByNode
}

// groupPartialTopCandidates agrupa los candidatos de Q4 por shard según el hash de la palabra.
func groupPartialTopCandidates(cleanVocabularies generic.Batch[*messages.ExtractedVocabulary], amount int) map[int]*generic.Batch[*messages.PartialTopCandidate] {
	candidatesByNode := make(map[int]*generic.Batch[*messages.PartialTopCandidate])
	for _, v := range cleanVocabularies.Items {
		for word := range v.Vocabulary {
			targetNode := utils.GetTargetNodeByWord(word, amount)
			if _, exists := candidatesByNode[targetNode]; !exists {
				batch := messages.NewEmptyPartialTopCandidateBatch(0)
				candidatesByNode[targetNode] = &batch
			}
			candidatesByNode[targetNode].Add(&messages.PartialTopCandidate{
				ArticleID: v.ArticleID,
				Word:      word,
			})
		}
	}
	return candidatesByNode
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
			URL:          abs.URL,
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
		cleanWord := strings.Trim(stopwords.CleanString(lowercaseWord, lang, false), PUNCTUATION_SYMBOLS)
		if _, exists := cleanVocabulary[cleanWord]; !exists && len(cleanWord) > 0 && cleanWord != " " {
			cleanVocabulary[cleanWord] = struct{}{}
		}
	}
	return cleanVocabulary
}

// getWordCounts genera los word counts para Q5.
func getWordCounts(cleanVocabularies generic.Batch[*messages.ExtractedVocabulary]) generic.Batch[*messages.WordCount] {
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
