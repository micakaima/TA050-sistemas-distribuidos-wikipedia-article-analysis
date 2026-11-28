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
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type Config struct {
	Id                    int
	MomHost               string
	MomPort               int
	ImageDownloaderPrefix string
	Q2AggregatorPrefix    string
	Q2AggregatorAmount    int
	BioFilterAmount       int
	MaxDownloadRetries    int
	ClientTimeout         int
	UserAgent             string
}

type ImageDownloaderWorker struct {
	inputExchange      middleware.Middleware
	outputQueue        middleware.Middleware
	recvEofCountMap    map[uint64]int
	bioFilterAmount    int
	downloader         *logic.Downloader
	maxDownloadRetries int
	clientTimeout      int
	TimeLimit          int
	userAgent          string
	q2AggregatorPrefix string
	q2AggregatorAmount int
}

func New(config Config) (*ImageDownloaderWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchangeRoutingKey := []string{utils.GenerateRoutingKey(config.ImageDownloaderPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.ImageDownloaderPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		return nil, err
	}
	outputExchange, err := middleware.CreateExchangeMiddleware(config.Q2AggregatorPrefix, []string{}, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	downloader := logic.NewDownloader(logic.DownloaderConfig{
		MaxRetries:    config.MaxDownloadRetries,
		ClientTimeout: config.ClientTimeout,
		UserAgent:     config.UserAgent,
		TimeLimit:     config.TimeLimit,
	})

	return &ImageDownloaderWorker{
		inputExchange:      inputExchange,
		outputQueue:        outputQueue,
		recvEofCountMap:    map[uint64]int{},
		bioFilterAmount:    config.BioFilterAmount,
		downloader:         downloader,
		maxDownloadRetries: config.MaxDownloadRetries,
		clientTimeout:      config.ClientTimeout,
		userAgent:          config.UserAgent,
		TimeLimit:          config.TimeLimit,
		q2AggregatorPrefix: config.Q2AggregatorPrefix,
		q2AggregatorAmount: config.Q2AggregatorAmount,
	}, nil
}

func (w *ImageDownloaderWorker) Run() {
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

func (w *ImageDownloaderWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.ImageCandidateCode:
		batch, err := messages.ImageCandidateBatchFromBytes([]byte(msg.Body))
		if err != nil {
			nack()
			return
		}
		result := w.downloader.DownloadBatch(*batch)
		resultMessage, err := middleware.NewMessage(msg.ClientID, messages.Q2ResultCode, &result)
		if err != nil {
			nack()
			return
		}
		targetNode := utils.GetTargetNodeByClientID(msg.ClientID, w.q2AggregatorAmount)
		routingKey := utils.GenerateRoutingKey(w.q2AggregatorPrefix, targetNode)
		if err := w.outputExchange.SendToKey(*resultMessage, routingKey); err != nil {
			nack()
			return
		}
	case messages.EOFCode:
		w.recvEofCountMap[msg.ClientID]++
		if w.recvEofCountMap[msg.ClientID] < w.bioFilterAmount {
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
		targetNode := utils.GetTargetNodeByClientID(msg.ClientID, w.q2AggregatorAmount)
		routingKey := utils.GenerateRoutingKey(w.q2AggregatorPrefix, targetNode)
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

func (w *ImageDownloaderWorker) shutdown() {
	if err := w.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
	if err := w.outputExchange.Close(); err != nil {
		slog.Error("While closing output exchange", "err", err)
	}
}
