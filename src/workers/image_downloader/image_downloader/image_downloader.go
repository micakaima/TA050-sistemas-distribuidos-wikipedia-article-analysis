package image_downloader

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
	OutputQueue        string
	MaxDownloadRetries int
	ClientTimeout      int
	UserAgent          string
}

type ImageDownloaderWorker struct {
	inputQueue         middleware.Middleware
	outputQueue        middleware.Middleware
	maxDownloadRetries int
	clientTimeout      int
	userAgent          string
}

func New(config Config) (*ImageDownloaderWorker, error) {
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
	return &ImageDownloaderWorker{
		inputQueue:         inputQueue,
		outputQueue:        outputQueue,
		maxDownloadRetries: config.MaxDownloadRetries,
		clientTimeout:      config.ClientTimeout,
		userAgent:          config.UserAgent,
	}, nil
}

func (w *ImageDownloaderWorker) Run() {
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

func (w *ImageDownloaderWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.ImageCandidateCode:
		batch, err := messages.ImageCandidateBatchFromBytes([]byte(msg.Body))
		if err != nil {
			nack()
			return
		}
		result := logic.DownloadImage(*batch, w.maxDownloadRetries, w.clientTimeout, w.userAgent)
		resultMessage, err := middleware.NewMessage(msg.ClientID, messages.Q2ResultCode, &result)
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

func (w *ImageDownloaderWorker) shutdown() {
	if err := w.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := w.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}
