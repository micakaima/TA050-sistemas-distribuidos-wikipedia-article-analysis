package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/image_downloader/image_downloader"
)

func loadConfig() (image_downloader.Config, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return image_downloader.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return image_downloader.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return image_downloader.Config{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return image_downloader.Config{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	maxDownloadRetries, err := strconv.Atoi(os.Getenv("MAX_DOWNLOAD_RETRIES"))
	if err != nil {
		return image_downloader.Config{}, errors.New("MAX_DOWNLOAD_RETRIES environment variable is required and must be a number")
	}

	clientTimeout, err := strconv.Atoi(os.Getenv("CLIENT_TIMEOUT"))
	if err != nil {
		return image_downloader.Config{}, errors.New("CLIENT_TIMEOUT environment variable is required and must be a number")
	}

	userAgent := os.Getenv("USER_AGENT")
	if userAgent == "" {
		return image_downloader.Config{}, errors.New("USER_AGENT environment variable is required")
	}

	return image_downloader.Config{
		MomHost:            momHost,
		MomPort:            momPort,
		InputQueue:         inputQueue,
		OutputQueue:        outputQueue,
		MaxDownloadRetries: maxDownloadRetries,
		ClientTimeout:      clientTimeout,
		UserAgent:          userAgent,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	worker, err := image_downloader.New(config)
	if err != nil {
		slog.Error("While initializing Image Downloader", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() { os.Exit(run()) }
