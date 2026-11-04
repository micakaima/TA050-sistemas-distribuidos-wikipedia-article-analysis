package logic

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	qresults "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
)

const (
	ExternalAPIError    = "External API error: could not retrieve image"
	InternalErrorPrefix = "Internal error: "
	NoThumbnailError    = "no thumbnail found"
)

type WikiSummaryResponse struct {
	Thumbnail *struct {
		Source string `json:"source"`
	} `json:"thumbnail"`
}

func calculateBackoff(attempt int) time.Duration {
	return time.Second * time.Duration(1<<uint(attempt)) // Exponential backoff: 1s, 2s, 4s, 8s, ...
}

func requestBytes(client *http.Client, targetUrl string, maxRetries int, userAgent string) ([]byte, error) {
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequest(http.MethodGet, targetUrl, nil)
		if err != nil {
			slog.Error("Failed to create HTTP request", "err", err)
			return nil, fmt.Errorf(InternalErrorPrefix + "failed to create HTTP request: " + err.Error())
		}

		req.Header.Set("User-Agent", userAgent)

		resp, err := client.Do(req)
		if err != nil {
			if attempt < maxRetries-1 {
				slog.Warn("HTTP request failed, retrying", "attempt", attempt+1, "err", err)
				time.Sleep(calculateBackoff(attempt))
				continue
			}
			break
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK && err == nil {
			return body, nil
		}

		slog.Warn("HTTP request failed", "status", resp.StatusCode, "attempt", attempt+1, "err", err)

		if attempt < maxRetries-1 {
			time.Sleep(calculateBackoff(attempt))
			continue
		}
		slog.Error("HTTP request failed after all retries", "status", resp.StatusCode, "err", err)
		break
	}
	return nil, fmt.Errorf(ExternalAPIError)
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
			result.Error = InternalErrorPrefix + "missing URL"
			results.Add(result)
			continue
		}

		splittedURL := strings.Split(r.URL, "/wiki/")
		if len(splittedURL) != 2 {
			slog.Error("Invalid URL format for article", "article_id", r.ArticleID, "name", r.Name, "url", r.URL)
			result.Error = InternalErrorPrefix + "invalid URL format"
			results.Add(result)
			continue
		}
		title := splittedURL[1]

		language := "en"
		if strings.Contains(r.URL, ".fr.wikipedia.org") {
			language = "fr"
		}

		apiURL := fmt.Sprintf("https://%s.wikipedia.org/api/rest_v1/page/summary/%s", language, url.PathEscape(title))

		bodyBytes, err := requestBytes(client, apiURL, maxRetries, userAgent)
		if err != nil {
			result.Error = err.Error()
			results.Add(result)
			continue
		}

		var summary WikiSummaryResponse

		if err := json.Unmarshal(bodyBytes, &summary); err != nil {
			slog.Error("Failed to unmarshal JSON response", "article_id", r.ArticleID, "name", r.Name, "err", err)
			result.Error = InternalErrorPrefix + "JSON unmarshal failed: " + err.Error()
			results.Add(result)
			continue
		}

		if summary.Thumbnail == nil || summary.Thumbnail.Source == "" {
			slog.Info("No thumbnail found for article", "article_id", r.ArticleID, "name", r.Name)
			result.Error = NoThumbnailError
			results.Add(result)
			continue
		}

		imageBytes, err := requestBytes(client, summary.Thumbnail.Source, maxRetries, userAgent)
		if err != nil {
			result.Error = err.Error()
			results.Add(result)
			continue
		}

		result.ImageBytes = imageBytes
		result.Error = ""
		results.Add(result)
	}
	return qresults.Q2Result{ResultArticles: results}
}
