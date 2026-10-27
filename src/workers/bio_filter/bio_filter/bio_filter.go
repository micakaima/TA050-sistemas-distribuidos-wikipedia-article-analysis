package bio_filter

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
	OutputQueue string
}

type BioFilterWorker struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
}

func New(config Config) (*BioFilterWorker, error) {
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
	return &BioFilterWorker{
		inputQueue:  inputQueue,
		outputQueue: outputQueue,
	}, nil
}

func (w *BioFilterWorker) Run() {
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

func (w *BioFilterWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	defer ack()
	records, err := record.FromBytes([]byte(msg.Body))
	if err != nil { 
		slog.Error("While parsing record", "err", err)
		return
	}

	result := logic.FilterByBio(records) 

	bytes, err := record.ToBytes(result)
	if err != nil {
		slog.Error("While serializing record", "err", err) 
		return
	}
	w.outputQueue.Send(middleware.Message{Body: string(bytes)})
}

func (w *BioFilterWorker) shutdown() {
	if err := w.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := w.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}
