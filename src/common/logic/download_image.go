package logic

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
)

type WikiSummaryResponse struct {
	Thumbnail *struct {
		Source string `json:"source"`
	} `json:"thumbnail"`
}

func requestBytes(client *http.Client, targetUrl string) ([]byte, error) {
	resp, err := client.Get(targetUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status: %s", resp.Status)
	}

	return io.ReadAll(resp.Body)
}

func DownloadImage(records generic.Batch[*messages.ImageCandidate]) generic.Batch[*messages.Q2Result] {
	client := &http.Client{Timeout: 10 * time.Second}
	results := messages.NewEmptyQ2ResultBatch(records.Len())

	for _, r := range records.Items {
		result := &messages.Q2Result{
			ArticleID: r.ArticleID,
			Name:      r.Name,
			URL:       r.URL,
		}
		if r.ArticleID == "" {
			result.Error = "missing identifier"
			results.Add(result)
			continue
		}

		apiURL := fmt.Sprintf("https://en.wikipedia.org/api/rest_v1/page/summary/%s", url.PathEscape(r.ArticleID))
		bodyBytes, err := requestBytes(client, apiURL)
		if err != nil {
			result.Error = "API request failed: " + err.Error()
			results.Add(result)
			continue
		}

		var summary WikiSummaryResponse

		if err := json.Unmarshal(bodyBytes, &summary); err != nil {
			result.Error = "JSON unmarshal failed: " + err.Error()
			results.Add(result)
			continue
		}

		if summary.Thumbnail == nil || summary.Thumbnail.Source == "" {
			result.Error = "no thumbnail found"
			results.Add(result)
			continue
		}

		imageBytes, err := requestBytes(client, summary.Thumbnail.Source)
		if err != nil {
			result.Error = "image download failed: " + err.Error()
			results.Add(result)
			continue
		}

		result.ImageBytes = imageBytes
		result.Error = ""
		results.Add(result)
	}
	return results
}
