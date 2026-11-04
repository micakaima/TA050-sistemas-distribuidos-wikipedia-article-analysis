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
	Id                        int
	MomHost                   string
	MomPort                   int
	ColumnFilterPrefix        string
	YearFilterAmount          int
	YearFilterPrefix          string
	VocabularyExtractorAmount int
	VocabularyExtractorPrefix string
}

type ColumnFilterWorker struct {
	inputExchange             middleware.Middleware
	outputExchangeQ1ToQ2      middleware.ExchangeMiddleware
	outputExchangeQ3ToQ5      middleware.ExchangeMiddleware
	yearFilterAmount          int
	yearFilterPrefix          string
	vocabularyExtractorAmount int
	vocabularyExtractorPrefix string
}

func New(config Config) (*ColumnFilterWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchangeRoutingKey := []string{utils.GenerateRoutingKey(config.ColumnFilterPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.ColumnFilterPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		return nil, err
	}
	outputExchangeQ1ToQ2, err := middleware.CreateExchangeMiddleware(config.YearFilterPrefix, []string{}, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}
	outputExchangeQ3ToQ5, err := middleware.CreateExchangeMiddleware(config.VocabularyExtractorPrefix, []string{}, connSettings)
	if err != nil {
		inputExchange.Close()
		outputExchangeQ1ToQ2.Close()
		return nil, err
	}
	return &ColumnFilterWorker{
		inputExchange:             inputExchange,
		outputExchangeQ1ToQ2:      outputExchangeQ1ToQ2,
		outputExchangeQ3ToQ5:      outputExchangeQ3ToQ5,
		yearFilterAmount:          config.YearFilterAmount,
		yearFilterPrefix:          config.YearFilterPrefix,
		vocabularyExtractorAmount: config.VocabularyExtractorAmount,
		vocabularyExtractorPrefix: config.VocabularyExtractorPrefix,
	}, nil
}

func (w *ColumnFilterWorker) Run() {
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

func (w *ColumnFilterWorker) handleSignals(done chan struct{}) {
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

func (w *ColumnFilterWorker) shutdown() {
	if err := w.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
	if err := w.outputExchangeQ1ToQ2.Close(); err != nil {
		slog.Error("While closing output exchange Q1ToQ2", "err", err)
	}
	if err := w.outputExchangeQ3ToQ5.Close(); err != nil {
		slog.Error("While closing output exchange Q3ToQ5", "err", err)
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

	// Agrupar YearCandidate por target node para Q1-Q2
	q1q2ByNode := make(map[int]generic.Batch[*messages.YearCandidate])
	for _, item := range recordsQ1ToQ2.Items {
		targetNode := utils.GetTargetNodeByArticleID(item.ArticleID, w.yearFilterAmount)
		b, exists := q1q2ByNode[targetNode]
		if !exists {
			b = messages.NewEmptyYearCandidateBatch(0)
		}
		b.Add(item)
		q1q2ByNode[targetNode] = b
	}
	for node, batch := range q1q2ByNode {
		q1q2Message, err := middleware.NewMessage(msg.ClientID, messages.YearCandidateCode, &batch)
		if err != nil {
			slog.Debug("While creating Year Candidate Batch message", "err", err)
			return err
		}
		routingKey := utils.GenerateRoutingKey(w.yearFilterPrefix, node)
		if err := w.outputExchangeQ1ToQ2.SendToKey(*q1q2Message, routingKey); err != nil {
			slog.Debug("While sending Year Candidate Batch message", "err", err)
			return err
		}
	}

	// Agrupar RawAbstract por target node para Q3-Q5
	q3q5ByNode := make(map[int]generic.Batch[*messages.RawAbstract])
	for _, item := range recordsQ3ToQ5.Items {
		targetNode := utils.GetTargetNodeByArticleID(item.ArticleID, w.vocabularyExtractorAmount)
		b, exists := q3q5ByNode[targetNode]
		if !exists {
			b = messages.NewEmptyRawAbstractBatch(0)
		}
		b.Add(item)
		q3q5ByNode[targetNode] = b
	}
	for node, batch := range q3q5ByNode {
		q3q5Message, err := middleware.NewMessage(msg.ClientID, messages.RawAbstractCode, &batch)
		if err != nil {
			slog.Debug("While creating Raw Abstract Batch message", "err", err)
			return err
		}
		routingKey := utils.GenerateRoutingKey(w.vocabularyExtractorPrefix, node)
		if err := w.outputExchangeQ3ToQ5.SendToKey(*q3q5Message, routingKey); err != nil {
			slog.Debug("While sending Raw Abstract Batch message", "err", err)
			return err
		}
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

	for n := 0; n < w.yearFilterAmount; n++ {
		routingKey := utils.GenerateRoutingKey(w.yearFilterPrefix, n)
		if err := w.outputExchangeQ1ToQ2.SendToKey(*eofMessage, routingKey); err != nil {
			slog.Debug("While sending EOF message to YearFilter", "err", err)
			return err
		}
	}

	for n := 0; n < w.vocabularyExtractorAmount; n++ {
		routingKey := utils.GenerateRoutingKey(w.vocabularyExtractorPrefix, n)
		if err := w.outputExchangeQ3ToQ5.SendToKey(*eofMessage, routingKey); err != nil {
			slog.Debug("While sending EOF message to VocabularyExtractor", "err", err)
			return err
		}
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
