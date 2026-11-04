package year_filter

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/logic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
)

type Config struct {
	MomHost            string
	MomPort            int
	InputQueue         string
	OutputExchange     string
	RoutingKeys        []string
	Year               int
	ColumnFilterAmount int
}

type YearFilterWorker struct {
	inputQueue         middleware.Middleware
	outputExchange     middleware.ExchangeMiddleware
	recvEofCountMap    map[uint64]int
	routingKeys        []string
	year               int
	columnFilterAmount int
}

func New(config Config) (*YearFilterWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.OutputExchange, config.RoutingKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}
	return &YearFilterWorker{
		inputQueue:         inputQueue,
		outputExchange:     outputExchange,
		recvEofCountMap:    map[uint64]int{},
		routingKeys:        config.RoutingKeys,
		year:               config.Year,
		columnFilterAmount: config.ColumnFilterAmount,
	}, nil
}

func (w *YearFilterWorker) Run() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		w.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			w.handleMessage(msg, ack, nack)
		})
	}()

	<-signals
	slog.Info("Graceful shutdown started...")
	w.inputQueue.StopConsuming()
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

	englishMessage, err := middleware.NewMessage(msg.ClientID, messages.EnglishCandidateCode, &englishCandidates)
	if err != nil {
		return err
	}
	if err := w.outputExchange.SendToKey(*englishMessage, w.routingKeys[0]); err != nil {
		return err
	}

	bioMessage, err := middleware.NewMessage(msg.ClientID, messages.BioCandidateCode, &bioCandidates)
	if err != nil {
		return err
	}
	return w.outputExchange.SendToKey(*bioMessage, w.routingKeys[1])
}

func (w *YearFilterWorker) handleEOF(msg middleware.Message) error {
	w.recvEofCountMap[msg.ClientID]++
	if w.recvEofCountMap[msg.ClientID] < w.columnFilterAmount {
		return nil
	}

	eof := messages.EofMsg{}
	eofMessage, err := middleware.NewMessage(msg.ClientID, messages.EOFCode, &eof)
	if err != nil {
		return err
	}
	return w.outputExchange.Send(*eofMessage)
}

func (w *YearFilterWorker) shutdown() {
	if err := w.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := w.outputExchange.Close(); err != nil {
		slog.Error("While closing output exchange", "err", err)
	}
}
