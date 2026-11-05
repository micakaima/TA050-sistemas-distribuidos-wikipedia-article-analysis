package year_filter

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/logic"
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type Config struct {
	Id                  int
	MomHost             string
	MomPort             int
	YearFilterPrefix    string
	EnglishFilterAmount int
	EnglishFilterPrefix string
	BioFilterAmount     int
	BioFilterPrefix     string
	Year                int
	ColumnFilterAmount  int
}

type YearFilterWorker struct {
	inputExchange         middleware.Middleware
	outputExchangeEnglish middleware.ExchangeMiddleware
	outputExchangeBio     middleware.ExchangeMiddleware
	recvEofCountMap       map[uint64]int
	year                  int
	columnFilterAmount    int
	englishFilterAmount   int
	englishFilterPrefix   string
	bioFilterAmount       int
	bioFilterPrefix       string
}

func New(config Config) (*YearFilterWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchangeRoutingKey := []string{utils.GenerateRoutingKey(config.YearFilterPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.YearFilterPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeEnglish, err := middleware.CreateExchangeMiddleware(config.EnglishFilterPrefix, []string{}, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	outputExchangeBio, err := middleware.CreateExchangeMiddleware(config.BioFilterPrefix, []string{}, connSettings)
	if err != nil {
		inputExchange.Close()
		outputExchangeEnglish.Close()
		return nil, err
	}

	return &YearFilterWorker{
		inputExchange:         inputExchange,
		outputExchangeEnglish: outputExchangeEnglish,
		outputExchangeBio:     outputExchangeBio,
		recvEofCountMap:       map[uint64]int{},
		year:                  config.Year,
		columnFilterAmount:    config.ColumnFilterAmount,
		englishFilterAmount:   config.EnglishFilterAmount,
		englishFilterPrefix:   config.EnglishFilterPrefix,
		bioFilterAmount:       config.BioFilterAmount,
		bioFilterPrefix:       config.BioFilterPrefix,
	}, nil
}

func (w *YearFilterWorker) Run() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		w.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			w.handleMessage(msg, ack, nack)
		})
	}()

	<-signals
	slog.Info("Graceful shutdown started...")
	w.inputExchange.StopConsuming()
	wg.Wait()
	w.shutdown()
}

func (w *YearFilterWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.YearCandidateCode:
		if err := w.handleYearCandidates(msg); err != nil {
			slog.Error("While handling YearCandidate batch", "err", err)
			nack()
			return
		}
	case messages.EOFCode:
		if err := w.handleEOF(msg); err != nil {
			slog.Error("While handling EOF", "err", err)
			nack()
			return
		}
	default:
		slog.Debug("Received unexpected inner message code", "code", msg.MsgCode)
		nack()
		return
	}
	ack()
}

func (w *YearFilterWorker) handleYearCandidates(msg middleware.Message) error {
	batch, err := messages.YearCandidateBatchFromBytes([]byte(msg.Body))
	if err != nil {
		return err
	}

	englishCandidates, bioCandidates := logic.FilterYearCandidates(*batch, w.year)

	// Agrupar EnglishCandidate por target node
	englishByNode := make(map[int]generic.Batch[*messages.EnglishCandidate])
	for _, item := range englishCandidates.Items {
		targetNode := utils.GetTargetNodeByArticleID(item.ArticleID, w.englishFilterAmount)
		b, exists := englishByNode[targetNode]
		if !exists {
			b = messages.NewEmptyEnglishCandidateBatch(0)
		}
		b.Add(item)
		englishByNode[targetNode] = b
	}
	for node, b := range englishByNode {
		englishMessage, err := middleware.NewMessage(msg.ClientID, messages.EnglishCandidateCode, &b)
		if err != nil {
			return err
		}
		routingKey := utils.GenerateRoutingKey(w.englishFilterPrefix, node)
		if err := w.outputExchangeEnglish.SendToKey(*englishMessage, routingKey); err != nil {
			return err
		}
	}

	// Agrupar BioCandidate por target node
	bioByNode := make(map[int]generic.Batch[*messages.BioCandidate])
	for _, item := range bioCandidates.Items {
		targetNode := utils.GetTargetNodeByArticleID(item.ArticleID, w.bioFilterAmount)
		b, exists := bioByNode[targetNode]
		if !exists {
			b = messages.NewEmptyBioCandidateBatch(0)
		}
		b.Add(item)
		bioByNode[targetNode] = b
	}
	for node, b := range bioByNode {
		bioMessage, err := middleware.NewMessage(msg.ClientID, messages.BioCandidateCode, &b)
		if err != nil {
			return err
		}
		routingKey := utils.GenerateRoutingKey(w.bioFilterPrefix, node)
		if err := w.outputExchangeBio.SendToKey(*bioMessage, routingKey); err != nil {
			return err
		}
	}

	return nil
}

func (w *YearFilterWorker) handleEOF(msg middleware.Message) error {
	w.recvEofCountMap[msg.ClientID]++
	if w.recvEofCountMap[msg.ClientID] < w.columnFilterAmount {
		return nil
	}
	delete(w.recvEofCountMap, msg.ClientID)

	eof := messages.EofMsg{}
	eofMessage, err := middleware.NewMessage(msg.ClientID, messages.EOFCode, &eof)
	if err != nil {
		return err
	}

	for n := 0; n < w.englishFilterAmount; n++ {
		routingKey := utils.GenerateRoutingKey(w.englishFilterPrefix, n)
		if err := w.outputExchangeEnglish.SendToKey(*eofMessage, routingKey); err != nil {
			return err
		}
	}

	for n := 0; n < w.bioFilterAmount; n++ {
		routingKey := utils.GenerateRoutingKey(w.bioFilterPrefix, n)
		if err := w.outputExchangeBio.SendToKey(*eofMessage, routingKey); err != nil {
			return err
		}
	}

	return nil
}

func (w *YearFilterWorker) shutdown() {
	if err := w.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
	if err := w.outputExchangeEnglish.Close(); err != nil {
		slog.Error("While closing english output exchange", "err", err)
	}
	if err := w.outputExchangeBio.Close(); err != nil {
		slog.Error("While closing bio output exchange", "err", err)
	}
}
