package bio_filter

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
	Id                    int
	MomHost               string
	MomPort               int
	BioFilterPrefix       string
	ImageDownloaderAmount int
	ImageDownloaderPrefix string
	YearFilterAmount      int
}

type BioFilterWorker struct {
	inputExchange         middleware.Middleware
	outputExchange        middleware.ExchangeMiddleware
	recvEofCountMap       map[uint64]int
	imageDownloaderAmount int
	imageDownloaderPrefix string
	yearFilterAmount      int
}

func New(config Config) (*BioFilterWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchangeRoutingKey := []string{utils.GenerateRoutingKey(config.BioFilterPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.BioFilterPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		return nil, err
	}
	outputExchange, err := middleware.CreateExchangeMiddleware(config.ImageDownloaderPrefix, []string{}, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}
	return &BioFilterWorker{
		inputExchange:         inputExchange,
		outputExchange:        outputExchange,
		recvEofCountMap:       map[uint64]int{},
		imageDownloaderAmount: config.ImageDownloaderAmount,
		imageDownloaderPrefix: config.ImageDownloaderPrefix,
		yearFilterAmount:      config.YearFilterAmount,
	}, nil
}

func (w *BioFilterWorker) Run() {
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

func (w *BioFilterWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.BioCandidateCode:
		batch, err := messages.BioCandidateBatchFromBytes([]byte(msg.Body))
		if err != nil {
			nack()
			return
		}
		result := logic.FilterBioCandidates(*batch)

		// Agrupar ImageCandidate por target node
		imagesByNode := make(map[int]generic.Batch[*messages.ImageCandidate])
		for _, item := range result.Items {
			targetNode := utils.GetTargetNodeByArticleID(item.ArticleID, w.imageDownloaderAmount)
			b, exists := imagesByNode[targetNode]
			if !exists {
				b = messages.NewEmptyImageCandidateBatch(0)
			}
			b.Add(item)
			imagesByNode[targetNode] = b
		}
		for node, b := range imagesByNode {
			resultMessage, err := middleware.NewMessage(msg.ClientID, messages.ImageCandidateCode, &b)
			if err != nil {
				nack()
				return
			}
			routingKey := utils.GenerateRoutingKey(w.imageDownloaderPrefix, node)
			if err := w.outputExchange.SendToKey(*resultMessage, routingKey); err != nil {
				nack()
				return
			}
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
		for n := 0; n < w.imageDownloaderAmount; n++ {
			routingKey := utils.GenerateRoutingKey(w.imageDownloaderPrefix, n)
			if err := w.outputExchange.SendToKey(*eofMessage, routingKey); err != nil {
				nack()
				return
			}
		}
	default:
		nack()
		return
	}
	ack()
}

func (w *BioFilterWorker) shutdown() {
	if err := w.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
	if err := w.outputExchange.Close(); err != nil {
		slog.Error("While closing output exchange", "err", err)
	}
}
