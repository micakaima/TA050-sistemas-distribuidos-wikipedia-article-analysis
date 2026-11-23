package image_downloader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/logic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

const HealthySentinelFile = "/tmp/healthy"

type SeedArticle struct {
	ID    string
	Title string
	Lang  string
}

// 25 figuras biográficas icónicas de Wikipedia con sus page IDs (identificadores estándar)
var defaultSeedArticles = []SeedArticle{
	{ID: "824", Title: "Alan_Turing", Lang: "en"},
	{ID: "18884", Title: "Marie_Curie", Lang: "en"},
	{ID: "736", Title: "Albert_Einstein", Lang: "en"},
	{ID: "1158", Title: "Ada_Lovelace", Lang: "en"},
	{ID: "21492", Title: "Nelson_Mandela", Lang: "en"},
	{ID: "14598", Title: "Isaac_Newton", Lang: "en"},
	{ID: "5040", Title: "Charles_Darwin", Lang: "en"},
	{ID: "12300", Title: "Galileo_Galilei", Lang: "en"},
	{ID: "21453", Title: "Nikola_Tesla", Lang: "en"},
	{ID: "18079", Title: "Leonardo_da_Vinci", Lang: "en"},
	{ID: "634", Title: "Aristotle", Lang: "en"},
	{ID: "1353", Title: "Alexander_the_Great", Lang: "en"},
	{ID: "16147", Title: "Julius_Caesar", Lang: "en"},
	{ID: "21214", Title: "Napoleon", Lang: "en"},
	{ID: "11867", Title: "George_Washington", Lang: "en"},
	{ID: "1456", Title: "Abraham_Lincoln", Lang: "en"},
	{ID: "19379", Title: "Mahatma_Gandhi", Lang: "en"},
	{ID: "19409", Title: "Martin_Luther_King_Jr.", Lang: "en"},
	{ID: "33215", Title: "Winston_Churchill", Lang: "en"},
	{ID: "25660", Title: "Queen_Victoria", Lang: "en"},
	{ID: "32897", Title: "William_Shakespeare", Lang: "en"},
	{ID: "32884", Title: "Wolfgang_Amadeus_Mozart", Lang: "en"},
	{ID: "17947", Title: "Ludwig_van_Beethoven", Lang: "en"},
	{ID: "32514", Title: "Vincent_van_Gogh", Lang: "en"},
	{ID: "23548", Title: "Pablo_Picasso", Lang: "en"},
}

type Config struct {
	Id                    int
	MomHost               string
	MomPort               int
	ImageDownloaderPrefix string
	OutputQueue           string
	BioFilterAmount       int
	MaxDownloadRetries    int
	ClientTimeout         int
	UserAgent             string
	ImageStorageDir       string
}

type ImageDownloaderWorker struct {
	inputExchange      middleware.Middleware
	outputQueue        middleware.Middleware
	recvEofCountMap    map[uint64]int
	bioFilterAmount    int
	maxDownloadRetries int
	clientTimeout      int
	userAgent          string
	storageDir         string
}

func New(config Config) (*ImageDownloaderWorker, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputExchangeRoutingKey := []string{utils.GenerateRoutingKey(config.ImageDownloaderPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.ImageDownloaderPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		return nil, err
	}
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}
	return &ImageDownloaderWorker{
		inputExchange:      inputExchange,
		outputQueue:        outputQueue,
		recvEofCountMap:    map[uint64]int{},
		bioFilterAmount:    config.BioFilterAmount,
		maxDownloadRetries: config.MaxDownloadRetries,
		clientTimeout:      config.ClientTimeout,
		userAgent:          config.UserAgent,
		storageDir:         config.ImageStorageDir,
	}, nil
}

func (w *ImageDownloaderWorker) Run() {
	// 1. Preparar storage de fallback y reportar salud antes de consumir de RabbitMQ
	w.prepareStorageAndHealthcheck()

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

func (w *ImageDownloaderWorker) prepareStorageAndHealthcheck() {
	if err := os.MkdirAll(w.storageDir, 0o755); err != nil {
		slog.Error("Failed to create storage directory", "storage_dir", w.storageDir, "err", err)
	}

	// Identificar artículos que no estén ya en disco (idempotencia instantánea ante reinicios en la demo)
	missing := make([]SeedArticle, 0, len(defaultSeedArticles))
	for _, sa := range defaultSeedArticles {
		imgPath := filepath.Join(w.storageDir, sa.ID+".img")
		noimgPath := filepath.Join(w.storageDir, sa.ID+".noimage")
		if _, err := os.Stat(imgPath); err == nil {
			continue
		}
		if _, err := os.Stat(noimgPath); err == nil {
			continue
		}
		missing = append(missing, sa)
	}

	// Si faltan artículos, descargarlos concurrentemente con límite estricto de tiempo (8 segundos)
	if len(missing) > 0 {
		slog.Info("Starting storage seeding for missing baseline articles", "missing_count", len(missing))
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()

		jobs := make(chan SeedArticle, len(missing))
		for _, sa := range missing {
			jobs <- sa
		}
		close(jobs)

		workerCount := 10
		if len(missing) < workerCount {
			workerCount = len(missing)
		}

		client := &http.Client{Timeout: 3 * time.Second}
		var wg sync.WaitGroup

		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case sa, ok := <-jobs:
						if !ok {
							return
						}
						w.downloadSeedArticle(ctx, client, sa)
					}
				}
			}()
		}
		wg.Wait()
	} else {
		// Smoke test rápido si los archivos ya existían
		client := &http.Client{Timeout: 2 * time.Second}
		req, err := http.NewRequest(http.MethodGet, "https://en.wikipedia.org/api/rest_v1/page/summary/Wikipedia", nil)
		if err == nil {
			req.Header.Set("User-Agent", w.userAgent)
			if resp, err := client.Do(req); err == nil {
				resp.Body.Close()
			}
		}
	}

	// Escribir archivo centinela de salud para Docker Healthcheck
	if err := os.WriteFile(HealthySentinelFile, []byte("ok"), 0o644); err != nil {
		slog.Error("Failed to write healthy sentinel file", "err", err)
	} else {
		slog.Info("Image Downloader storage prepared and healthy", "storage_dir", w.storageDir, "cached_seed_count", len(defaultSeedArticles)-len(missing))
	}
}

func (w *ImageDownloaderWorker) downloadSeedArticle(ctx context.Context, client *http.Client, sa SeedArticle) {
	apiURL := fmt.Sprintf("https://%s.wikipedia.org/api/rest_v1/page/summary/%s", sa.Lang, url.PathEscape(sa.Title))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", w.userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	var summary struct {
		Thumbnail *struct {
			Source string `json:"source"`
		} `json:"thumbnail"`
	}
	if err := json.Unmarshal(body, &summary); err != nil {
		return
	}

	if summary.Thumbnail == nil || summary.Thumbnail.Source == "" {
		noimgPath := filepath.Join(w.storageDir, sa.ID+".noimage")
		_ = os.WriteFile(noimgPath, []byte(logic.ExternalAPIError), 0o644)
		return
	}

	imgReq, err := http.NewRequestWithContext(ctx, http.MethodGet, summary.Thumbnail.Source, nil)
	if err != nil {
		return
	}
	imgReq.Header.Set("User-Agent", w.userAgent)

	imgResp, err := client.Do(imgReq)
	if err != nil {
		return
	}
	defer imgResp.Body.Close()

	if imgResp.StatusCode == http.StatusOK {
		imgBytes, err := io.ReadAll(imgResp.Body)
		if err == nil && len(imgBytes) > 0 {
			imgPath := filepath.Join(w.storageDir, sa.ID+".img")
			_ = os.WriteFile(imgPath, imgBytes, 0o644)
		}
	}
}

func (w *ImageDownloaderWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.ImageCandidateCode:
		batch, err := messages.ImageCandidateBatchFromBytes([]byte(msg.Body))
		if err != nil {
			nack()
			return
		}
		result := logic.DownloadImage(*batch, w.maxDownloadRetries, w.clientTimeout, w.userAgent, w.storageDir)
		resultMessage, err := middleware.NewMessage(msg.ClientID, messages.Q2ResultCode, &result)
		if err != nil || w.outputQueue.Send(*resultMessage) != nil {
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
	_ = os.Remove(HealthySentinelFile)
	if err := w.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
	if err := w.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}
