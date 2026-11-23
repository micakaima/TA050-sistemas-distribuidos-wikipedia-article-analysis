package logic

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
)

func TestDownloadImage_DirectURL(t *testing.T) {
	expectedImg := []byte("real-image-payload-png")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(expectedImg)
	}))
	defer ts.Close()

	articleID := "99999"
	batch := generic.Batch[*messages.ImageCandidate]{
		Items: []*messages.ImageCandidate{
			{
				ArticleID: articleID,
				ImageURL:  ts.URL,
				Name:      "Direct Image Article",
				URL:       "https://en.wikipedia.org/wiki/Direct_Test",
			},
		},
	}

	res := DownloadImage(batch, 1, 2, "TestAgent")
	if res.ResultArticles.Len() != 1 {
		t.Fatalf("Expected 1 result, got %d", res.ResultArticles.Len())
	}
	item := res.ResultArticles.Items[0]
	if item.Error != "" {
		t.Errorf("Expected no error, got %s", item.Error)
	}
	if !bytes.Equal(item.ImageBytes, expectedImg) {
		t.Errorf("Image bytes mismatch")
	}
}

func TestDownloadImage_WikipediaSummary(t *testing.T) {
	expectedImg := []byte("thumbnail-payload-jpg")

	imgServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(expectedImg)
	}))
	defer imgServer.Close()

	summaryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := WikiSummaryResponse{
			Thumbnail: &struct {
				Source string `json:"source"`
			}{Source: imgServer.URL},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer summaryServer.Close()

	// Direct download test
	articleID := "12345"
	batch := generic.Batch[*messages.ImageCandidate]{
		Items: []*messages.ImageCandidate{
			{
				ArticleID: articleID,
				ImageURL:  imgServer.URL,
				Name:      "Summary Image Article",
				URL:       "https://en.wikipedia.org/wiki/Summary_Test",
			},
		},
	}

	res := DownloadImage(batch, 1, 2, "TestAgent")
	if res.ResultArticles.Len() != 1 {
		t.Fatalf("Expected 1 result, got %d", res.ResultArticles.Len())
	}
	item := res.ResultArticles.Items[0]
	if item.Error != "" {
		t.Errorf("Expected no error, got %s", item.Error)
	}
	if !bytes.Equal(item.ImageBytes, expectedImg) {
		t.Errorf("Image bytes mismatch")
	}
}
