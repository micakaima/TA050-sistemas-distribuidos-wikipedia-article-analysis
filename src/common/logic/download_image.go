package logic

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	qresults "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
)

const (
	ExternalAPIError       string = "External API error: %s"
	InternalErrorPrefix    string = "Internal error: "
	Error404Error          string = "Status Not Found (Error 404)"
	FailedAllAttemptsError string = "Failed all attempts for download"
	MissingImageURLError   string = "Missing Image URL Error"
)

type DownloaderConfig struct {
	MaxRetries    int
	ClientTimeout int
	UserAgent     string
	TimeLimit     int
}

type Downloader struct {
	client     *http.Client
	maxRetries int
	userAgent  string
	timeLimit  time.Duration
	cooldown   time.Time
}

func NewDownloader(config DownloaderConfig) *Downloader {
	client := &http.Client{Timeout: time.Duration(config.ClientTimeout) * time.Second}
	timeLimit := time.Duration(config.TimeLimit) * time.Second
	return &Downloader{
		client:     client,
		maxRetries: config.MaxRetries,
		userAgent:  config.UserAgent,
		timeLimit:  timeLimit,
	}
}

func (d *Downloader) sleepIfCanRetry(attempt int, wait time.Duration, waited *time.Duration) bool {
	if attempt >= d.maxRetries-1 {
		return false
	}
	if *waited+wait > d.timeLimit {
		return false
	}
	*waited += wait
	time.Sleep(wait)
	return true
}

func (d *Downloader) calculateBackoff(attempt int, articleID int) time.Duration {
	const (
		maxAttempts = 10
		baseBackoff = time.Second
		maxJitterMs = int(time.Second / time.Millisecond)
	)
	minAttempt := min(attempt, maxAttempts)
	jitter := time.Duration(articleID%maxJitterMs) * time.Millisecond
	return min((baseBackoff<<minAttempt)+jitter, d.timeLimit) // Exponential backoff: 1s, 2s, 4s, 8s, ...
}

func (d *Downloader) areTooManyRequests(resp *http.Response, articleID int, attempt int, waited *time.Duration) (handled bool, err error) {
	if resp.StatusCode != http.StatusTooManyRequests {
		return false, nil
	}

	retryAfterHeader := resp.Header.Get("Retry-After")
	if retryAfterHeader == "" {
		return false, nil
	}

	retryAfterSeconds, err := strconv.Atoi(retryAfterHeader)
	if err != nil || retryAfterSeconds <= 0 {
		return false, nil
	}

	retryAfter := time.Duration(retryAfterSeconds) * time.Second
	if retryAfter > d.timeLimit {
		d.cooldown = time.Now().Add(retryAfter)
		slog.Warn("Received 429 Too Many Requests, Retry-After exceeds timeLimit", "article_id", articleID, "retry_after", retryAfter)
		return true, fmt.Errorf(ExternalAPIError, fmt.Sprintf("Rate limit exceeded. Retry after %s", retryAfter))
	}

	if !d.sleepIfCanRetry(attempt, retryAfter, waited) {
		d.cooldown = time.Now().Add(retryAfter)
		return true, fmt.Errorf(ExternalAPIError, fmt.Sprintf("Rate limit exceeded. Retry after %s", retryAfter))
	}

	slog.Warn("Received 429 Too Many Requests, retrying after specified time", "article_id", articleID, "retry_after", retryAfter)
	return true, nil
}

func (d *Downloader) requestBytesFromAPI(targetUrl string, articleID int) ([]byte, error) {
	if time.Until(d.cooldown) > 0 {
		return nil, fmt.Errorf(ExternalAPIError, "Exceeded time limit for retries")
	}

	var waited time.Duration

	for attempt := 0; attempt < d.maxRetries; attempt++ {
		req, err := http.NewRequest(http.MethodGet, targetUrl, nil)
		if err != nil {
			slog.Error("Failed to create HTTP request", "err", err)
			return nil, fmt.Errorf(ExternalAPIError, err.Error())
		}

		req.Header.Set("User-Agent", d.userAgent)

		resp, err := d.client.Do(req)
		if err != nil {
			if attempt < d.maxRetries-1 {
				slog.Warn("HTTP request failed, retrying", "attempt", attempt+1, "err", err)
				if !d.sleepIfCanRetry(attempt, d.calculateBackoff(attempt, articleID), &waited) {
					break
				}
				continue
			}
			break
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK && err == nil {
			return body, nil
		}

		if resp.StatusCode == http.StatusNotFound {
			slog.Info("Resource not found", "status", resp.StatusCode, "article_id", articleID)
			return nil, fmt.Errorf(ExternalAPIError, Error404Error)
		}

		handled, rateLimitErr := d.areTooManyRequests(resp, articleID, attempt, &waited)
		if rateLimitErr != nil {
			return nil, rateLimitErr
		}
		if handled {
			continue
		}

		slog.Warn("HTTP request failed", "status", resp.StatusCode, "attempt", attempt+1, "err", err)
		if !d.sleepIfCanRetry(attempt, d.calculateBackoff(attempt, articleID), &waited) {
			slog.Error("HTTP request failed after all retries", "status", resp.StatusCode, "err", err)
			break
		}
	}
	return nil, fmt.Errorf(ExternalAPIError, FailedAllAttemptsError)

}

func (d *Downloader) searchBytesFromFallback(articleID int) ([]byte, error) {
	// TODO: Implement fallback search logic
	return nil, fmt.Errorf(ExternalAPIError, "Fallback search not implemented")
}

func (d *Downloader) DownloadBatch(records generic.Batch[*messages.ImageCandidate]) qresults.Q2Result {
	results := qresults.NewEmptyImageArticleBatch(0)

	for _, r := range records.Items {
		result := &qresults.ImageArticle{
			ArticleID: r.ArticleID,
			Name:      r.Name,
			URL:       r.URL,
		}
		if r.URL == "" {
			slog.Error("Missing URL for article", "article_id", r.ArticleID, "name", r.Name)
			result.Error = InternalErrorPrefix
			results.Add(result)
			continue
		}
		id, err := strconv.Atoi(r.ArticleID)
		if err != nil {
			slog.Error("Invalid ArticleID format", "article_id", r.ArticleID, "name", r.Name, "err", err)
			result.Error = InternalErrorPrefix
			results.Add(result)
			continue
		}

		if r.ImageURL == "" {
			result.Error = fmt.Errorf(ExternalAPIError, MissingImageURLError).Error()
			results.Add(result)
			continue
		} else {
			imageBytes, apiErr := d.requestBytesFromAPI(r.ImageURL, id)
			if apiErr == nil {
				result.ImageBytes = imageBytes
				result.Error = ""
				results.Add(result)
				continue
			}
			slog.Warn("Failed to download image from ImageURL", "article_id", r.ArticleID, "name", r.Name, "err", apiErr)
		}

		if fallbackBytes, fbErr := d.searchBytesFromFallback(id); fbErr == nil {
			result.ImageBytes = fallbackBytes
			result.Error = ""
			results.Add(result)
		} else {
			slog.Error("Failed to download image from fallback URL", "article_id", r.ArticleID, "name", r.Name, "err", fbErr)
			result.Error = fmt.Errorf(ExternalAPIError, Error404Error).Error()
			results.Add(result)
		}
	}
	return qresults.Q2Result{ResultArticles: results}
}
