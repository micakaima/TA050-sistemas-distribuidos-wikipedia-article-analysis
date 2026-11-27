package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/image_downloader/image_downloader"
)

func loadConfig() (image_downloader.Config, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return image_downloader.Config{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return image_downloader.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return image_downloader.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	imageDownloaderPrefix := os.Getenv("IMAGE_DOWNLOADER_PREFIX")
	if imageDownloaderPrefix == "" {
		return image_downloader.Config{}, errors.New("IMAGE_DOWNLOADER_PREFIX environment variable is required")
	}

	q2AggregatorPrefix := os.Getenv("Q2_AGGREGATOR_PREFIX")
	if q2AggregatorPrefix == "" {
		return image_downloader.Config{}, errors.New("Q2_AGGREGATOR_PREFIX environment variable is required")
	}

	q2AggregatorAmount, err := strconv.Atoi(os.Getenv("Q2_AGGREGATOR_AMOUNT"))
	if err != nil {
		return image_downloader.Config{}, errors.New("Q2_AGGREGATOR_AMOUNT environment variable is required and must be a number")
	}

	bioFilterAmount, err := strconv.Atoi(os.Getenv("BIO_FILTER_AMOUNT"))
	if err != nil {
		return image_downloader.Config{}, errors.New("BIO_FILTER_AMOUNT environment variable is required and must be a number")
	}

	maxDownloadRetries, err := strconv.Atoi(os.Getenv("MAX_DOWNLOAD_RETRIES"))
	if err != nil || maxDownloadRetries <= 0 {
		return image_downloader.Config{}, errors.New("MAX_DOWNLOAD_RETRIES environment variable is required and must be a positive number")
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
		Id:                    id,
		MomHost:               momHost,
		MomPort:               momPort,
		ImageDownloaderPrefix: imageDownloaderPrefix,
		Q2AggregatorPrefix:    q2AggregatorPrefix,
		Q2AggregatorAmount:    q2AggregatorAmount,
		BioFilterAmount:       bioFilterAmount,
		MaxDownloadRetries:    maxDownloadRetries,
		ClientTimeout:         clientTimeout,
		UserAgent:             userAgent,
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
