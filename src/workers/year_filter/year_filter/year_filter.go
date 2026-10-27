package year_filter

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"src/common/logic"
	"src/common/middleware"
	"src/common/record"
)

type Config struct {
	MomHost     string
	MomPort     int
	InputQueue  string
	OutputExchange string
	RoutingKeys []string
	Year        int
}

type YearFilterWorker struct {
	inputQueue        middleware.Middleware
	outputExchange    middleware.Middleware
	year 			  int
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
		inputQueue:  inputQueue,
		outputExchange: outputExchange,
		year:        config.Year,
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
	defer ack()
	records, err := record.FromBytes([]byte(msg.Body))
	if err != nil { 
		slog.Error("While parsing record", "err", err)
		return
	}

	result := logic.FilterByYear(records, w.year) 

	bytes, err := record.ToBytes(result)
	if err != nil {
		slog.Error("While serializing record", "err", err) 
		return
	}
	w.outputExchange.Send(middleware.Message{Body: string(bytes)})
}

func (w *YearFilterWorker) shutdown() {
	if err := w.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := w.outputExchange.Close(); err != nil {
		slog.Error("While closing output exchange", "err", err)
	}
}
