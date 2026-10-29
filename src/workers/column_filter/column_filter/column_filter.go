package column_filter

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/logic"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/record"
)

type Config struct {
	MomHost      string
	MomPort      int
	InputQueue   string
	OutputQ1ToQ2 string
	OutputQ3ToQ5 string
}

type ColumnFilterWorker struct {
	inputQueue   middleware.Middleware
	outputQ1ToQ2 middleware.Middleware
	outputQ3ToQ5 middleware.Middleware
}

func New(config Config) (*ColumnFilterWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	outputQ1ToQ2, err := middleware.CreateQueueMiddleware(config.OutputQ1ToQ2, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}
	outputQ3ToQ5, err := middleware.CreateQueueMiddleware(config.OutputQ3ToQ5, connSettings)
	if err != nil {
		inputQueue.Close()
		outputQ1ToQ2.Close()
		return nil, err
	}
	return &ColumnFilterWorker{
		inputQueue:   inputQueue,
		outputQ1ToQ2: outputQ1ToQ2,
		outputQ3ToQ5: outputQ3ToQ5,
	}, nil
}

func (w *ColumnFilterWorker) Run() {
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

func (w *ColumnFilterWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	defer ack()
	records, err := record.FromBytes[record.Record]([]byte(msg.Body))
	if err != nil {
		slog.Error("While parsing record", "err", err)
		return
	}

	q1q2, q3q5 := logic.FilterByQuery(records)

	bytesq1q2, err := record.ToBytes(q1q2)
	if err != nil {
		slog.Error("While serializing record", "err", err)
		return
	}
	w.outputQ1ToQ2.Send(middleware.Message{ClientID: msg.ClientID, Body: string(bytesq1q2)})

	bytesq3q5, err := record.ToBytes(q3q5)
	if err != nil {
		slog.Error("While serializing record", "err", err)
		return
	}
	w.outputQ3ToQ5.Send(middleware.Message{ClientID: msg.ClientID, Body: string(bytesq3q5)})
}

func (w *ColumnFilterWorker) shutdown() {
	if err := w.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := w.outputQ1ToQ2.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
	if err := w.outputQ3ToQ5.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}
