package logic

import (
	"errors"
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
	ExternalAPIError    = "External API error: could not retrieve image"
	InternalErrorPrefix = "Internal error: "
)

type WikiSummaryResponse struct {
	Thumbnail *struct {
		Source string `json:"source"`
	} `json:"thumbnail"`
}

func calculateBackoff(attempt int, articleID int) time.Duration {
	const (
		maxAttempts = 10
		baseBackoff = time.Second
		maxJitterMs = int(time.Second / time.Millisecond)
	)
	minAttempt := min(attempt, maxAttempts)
	jitter := time.Duration(articleID%maxJitterMs) * time.Millisecond
	return (baseBackoff << minAttempt) + jitter // Exponential backoff: 1s, 2s, 4s, 8s, ...
}

func areTooManyRequests(resp *http.Response, articleID int) bool {
	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := resp.Header.Get("Retry-After")
		if retryAfter != "" {
			retryAfterSeconds, err := strconv.Atoi(retryAfter)
			if err == nil {
				slog.Warn("Received 429 Too Many Requests, retrying after specified time", "article_id", articleID, "retry_after", retryAfterSeconds)
				time.Sleep(time.Duration(retryAfterSeconds) * time.Second)
				return true
			}
		}
	}
	return false
}

func requestBytes(client *http.Client, targetUrl string, maxRetries int, userAgent string, articleID int) ([]byte, error) {
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequest(http.MethodGet, targetUrl, nil)
		if err != nil {
			slog.Error("Failed to create HTTP request", "err", err)
			return nil, fmt.Errorf(InternalErrorPrefix)
		}

		req.Header.Set("User-Agent", userAgent)

		resp, err := client.Do(req)
		if err != nil {
			if attempt < maxRetries-1 {
				slog.Warn("HTTP request failed, retrying", "attempt", attempt+1, "err", err)
				time.Sleep(calculateBackoff(attempt, articleID))
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
			return nil, errors.New(ExternalAPIError)
		}

		slog.Warn("HTTP request failed", "status", resp.StatusCode, "attempt", attempt+1, "err", err)

		if attempt < maxRetries-1 {
			time.Sleep(calculateBackoff(attempt, articleID))
			continue
		}
		slog.Error("HTTP request failed after all retries", "status", resp.StatusCode, "err", err)
		break
	}
	return nil, errors.New(ExternalAPIError)
}

func DownloadImage(records generic.Batch[*messages.ImageCandidate], maxRetries int, clientTimeout int, userAgent string) qresults.Q2Result {
	client := &http.Client{Timeout: time.Duration(clientTimeout) * time.Second}
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
			slog.Info("Missing ImageURL for article", "article_id", r.ArticleID, "name", r.Name)
			result.Error = ExternalAPIError
			results.Add(result)
			continue
		}

		imageBytes, err := requestBytes(client, r.ImageURL, maxRetries, userAgent, id)
		if err != nil {
			slog.Warn("Failed to download image from ImageURL", "article_id", r.ArticleID, "name", r.Name, "err", err)
			result.Error = ExternalAPIError
			results.Add(result)
			continue
		}

		result.ImageBytes = imageBytes
		result.Error = ""
		results.Add(result)
	}
	return qresults.Q2Result{ResultArticles: results}
}
