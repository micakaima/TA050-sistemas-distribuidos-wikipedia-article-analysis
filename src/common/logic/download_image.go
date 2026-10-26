package logic

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"src/common/record"
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

func DownloadImage(records []record.Record) []record.Record {
	client := &http.Client{Timeout: 10 * time.Second}

	for i := range records {
		if records[i].Identifier == "" {
			records[i].Image.Error = "missing identifier"
			continue
		}

		apiURL := fmt.Sprintf("https://en.wikipedia.org/api/rest_v1/page/summary/%s", url.PathEscape(records[i].Identifier))
		bodyBytes, err := requestBytes(client, apiURL)
		if err != nil {
			records[i].Image.Error = "API request failed: " + err.Error()
			continue
		}

		var summary WikiSummaryResponse

		if err := json.Unmarshal(bodyBytes, &summary); err != nil {
			records[i].Image.Error = "JSON unmarshal failed: " + err.Error()
			continue
		}

		if summary.Thumbnail == nil || summary.Thumbnail.Source == "" {
			records[i].Image.Error = "no thumbnail found"
			continue
		}

		imageBytes, err := requestBytes(client, summary.Thumbnail.Source)
		if err != nil {
			records[i].Image.Error = "image download failed: " + err.Error()
			continue
		}

		records[i].Image.Bytes = imageBytes	
		records[i].Image.Error = ""
	}
	return records
}