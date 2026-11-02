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
)

type Config struct {
	MomHost       string
	MomPort       int
	RoutingKeys   []string
	InputExchange string
	OutputQueue   string
}

type EnglishFilterWorker struct {
	inputExchange middleware.Middleware
	outputQueue   middleware.Middleware
}

func New(config Config) (*EnglishFilterWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchange, config.RoutingKeys, connSettings)
	if err != nil {
		return nil, err
	}
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}
	return &EnglishFilterWorker{
		inputExchange: inputExchange,
		outputQueue:   outputQueue,
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
		if err != nil || w.outputQueue.Send(*resultMessage) != nil {
			nack()
			return
		}
	case messages.EOFCode:
		eof := messages.EofMsg{}
		eofMessage, err := middleware.NewMessage(msg.ClientID, messages.EOFCode, &eof)
		if err != nil || w.outputQueue.Send(*eofMessage) != nil {
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
	if err := w.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}
