package logic

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
)

func TestDownloadImage_FallbackStorage(t *testing.T) {
	tempStorageDir := t.TempDir()

	// 1. Crear archivo .img simulado en storage
	imgID := "12345"
	expectedBytes := []byte("fake-image-bytes-jpeg")
	imgPath := filepath.Join(tempStorageDir, imgID+".img")
	if err := os.WriteFile(imgPath, expectedBytes, 0o644); err != nil {
		t.Fatalf("Failed to write test img: %v", err)
	}

	// 2. Crear archivo .noimage simulado en storage
	noimgID := "67890"
	noimgPath := filepath.Join(tempStorageDir, noimgID+".noimage")
	if err := os.WriteFile(noimgPath, []byte(ExternalAPIError), 0o644); err != nil {
		t.Fatalf("Failed to write test noimage: %v", err)
	}

	batch := generic.Batch[*messages.ImageCandidate]{
		Items: []*messages.ImageCandidate{
			{
				ArticleID: imgID,
				Name:      "Article With Image",
				URL:       "https://en.wikipedia.org/wiki/Test_1",
			},
			{
				ArticleID: noimgID,
				Name:      "Article Without Image",
				URL:       "https://en.wikipedia.org/wiki/Test_2",
			},
		},
	}

	res := DownloadImage(batch, 1, 1, "TestAgent", tempStorageDir)
	if res.ResultArticles.Len() != 2 {
		t.Fatalf("Expected 2 results, got %d", res.ResultArticles.Len())
	}

	// Verificar item 1 (recuperado de fallback storage)
	item1 := res.ResultArticles.Items[0]
	if item1.ArticleID != imgID {
		t.Errorf("Expected article ID %s, got %s", imgID, item1.ArticleID)
	}
	if item1.Error != "" {
		t.Errorf("Expected no error for cached image, got %s", item1.Error)
	}
	if !bytes.Equal(item1.ImageBytes, expectedBytes) {
		t.Errorf("Image bytes mismatch. Expected %s, got %s", expectedBytes, item1.ImageBytes)
	}

	// Verificar item 2 (recuperado .noimage de fallback storage)
	item2 := res.ResultArticles.Items[1]
	if item2.ArticleID != noimgID {
		t.Errorf("Expected article ID %s, got %s", noimgID, item2.ArticleID)
	}
	if item2.Error != ExternalAPIError {
		t.Errorf("Expected error %s for noimage, got %s", ExternalAPIError, item2.Error)
	}
	if len(item2.ImageBytes) != 0 {
		t.Errorf("Expected 0 image bytes for noimage, got %d", len(item2.ImageBytes))
	}
}

func TestDownloadImage_DirectURL_SavesToStorage(t *testing.T) {
	tempStorageDir := t.TempDir()
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

	res := DownloadImage(batch, 1, 2, "TestAgent", tempStorageDir)
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

	// Verificar que se guardó en storage para futuros fallbacks
	savedPath := filepath.Join(tempStorageDir, fmt.Sprintf("%s.img", articleID))
	savedBytes, err := os.ReadFile(savedPath)
	if err != nil {
		t.Fatalf("File was not saved to storage: %v", err)
	}
	if !bytes.Equal(savedBytes, expectedImg) {
		t.Errorf("Saved bytes mismatch")
	}
}
