package english_filter

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/logic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type Config struct {
	Id                  int
	MomHost             string
	MomPort             int
	EnglishFilterPrefix string
	Q1AggregatorPrefix  string
	Q1AggregatorAmount  int
	YearFilterAmount    int
}

type EnglishFilterWorker struct {
	inputExchange      middleware.Middleware
	outputExchange     middleware.ExchangeMiddleware
	recvEofCountMap    map[uint64]int
	yearFilterAmount   int
	q1AggregatorAmount int
	q1AggregatorPrefix string
}

func New(config Config) (*EnglishFilterWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchangeRoutingKey := []string{utils.GenerateRoutingKey(config.EnglishFilterPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.EnglishFilterPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		return nil, err
	}
	outputExchange, err := middleware.CreateExchangeMiddleware(config.Q1AggregatorPrefix, []string{}, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}
	return &EnglishFilterWorker{
		inputExchange:      inputExchange,
		outputExchange:     outputExchange,
		recvEofCountMap:    map[uint64]int{},
		yearFilterAmount:   config.YearFilterAmount,
		q1AggregatorAmount: config.Q1AggregatorAmount,
		q1AggregatorPrefix: config.Q1AggregatorPrefix,
	}, nil
}

func (w *EnglishFilterWorker) Run() {
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

func (w *EnglishFilterWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.EnglishCandidateCode:
		batch, err := messages.EnglishCandidateBatchFromBytes([]byte(msg.Body))
		if err != nil {
			nack()
			return
		}
		result := logic.FilterEnglishCandidates(*batch)
		resultMessage, err := middleware.NewMessage(msg.ClientID, messages.Q1ResultCode, &result)
		if err != nil {
			nack()
			return
		}
		targetNode := utils.GetTargetNodeByClientID(msg.ClientID, w.q1AggregatorAmount)
		routingKey := utils.GenerateRoutingKey(w.q1AggregatorPrefix, targetNode)
		if err := w.outputExchange.SendToKey(*resultMessage, routingKey); err != nil {
			nack()
			return
		}
	case messages.EOFCode:
		w.recvEofCountMap[msg.ClientID]++
		if w.recvEofCountMap[msg.ClientID] < w.yearFilterAmount {
			ack()
			return
		}
		delete(w.recvEofCountMap, msg.ClientID)

		eof := messages.EofMsg{}
		eofMessage, err := middleware.NewMessage(msg.ClientID, messages.EOFCode, &eof)
		if err != nil {
			nack()
			return
		}
		targetNode := utils.GetTargetNodeByClientID(msg.ClientID, w.q1AggregatorAmount)
		routingKey := utils.GenerateRoutingKey(w.q1AggregatorPrefix, targetNode)
		if err := w.outputExchange.SendToKey(*eofMessage, routingKey); err != nil {
			nack()
			return
		}
	default:

		nack()
		return
	}
	ack()
}

func (w *EnglishFilterWorker) shutdown() {
	if err := w.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
	if err := w.outputExchange.Close(); err != nil {
		slog.Error("While closing output exchange", "err", err)
	}
}
