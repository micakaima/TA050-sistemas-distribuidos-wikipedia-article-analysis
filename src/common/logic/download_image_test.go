package logic

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
)

func TestDownloadImage_CacheReadOnly(t *testing.T) {
	tempCacheDir := t.TempDir()

	// 1. Crear archivo .img simulado
	imgID := "12345"
	expectedBytes := []byte("fake-image-bytes-jpeg")
	imgPath := filepath.Join(tempCacheDir, imgID+".img")
	if err := os.WriteFile(imgPath, expectedBytes, 0o644); err != nil {
		t.Fatalf("Failed to write test img: %v", err)
	}

	// 2. Crear archivo .noimage simulado
	noimgID := "67890"
	noimgPath := filepath.Join(tempCacheDir, noimgID+".noimage")
	if err := os.WriteFile(noimgPath, []byte("404 Not Found"), 0o644); err != nil {
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

	res := DownloadImage(batch, 1, 1, "TestAgent", tempCacheDir)
	if res.ResultArticles.Len() != 2 {
		t.Fatalf("Expected 2 results, got %d", res.ResultArticles.Len())
	}

	// Verificar item 1 (.img)
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

	// Verificar item 2 (.noimage)
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

	// Verificar que el directorio de caché no contenga archivos adicionales (modo solo lectura)
	files, err := os.ReadDir(tempCacheDir)
	if err != nil {
		t.Fatalf("Failed to read cache dir: %v", err)
	}
	if len(files) != 2 {
		t.Errorf("Cache directory was modified! Expected 2 files, got %d", len(files))
	}
}
