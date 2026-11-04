package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/bio_filter/bio_filter"
)

func loadConfig() (bio_filter.Config, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return bio_filter.Config{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return bio_filter.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return bio_filter.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	bioFilterPrefix := os.Getenv("BIO_FILTER_PREFIX")
	if bioFilterPrefix == "" {
		return bio_filter.Config{}, errors.New("BIO_FILTER_PREFIX environment variable is required")
	}

	imageDownloaderAmount, err := strconv.Atoi(os.Getenv("IMAGE_DOWNLOADER_AMOUNT"))
	if err != nil {
		return bio_filter.Config{}, errors.New("IMAGE_DOWNLOADER_AMOUNT environment variable is required and must be a number")
	}

	imageDownloaderPrefix := os.Getenv("IMAGE_DOWNLOADER_PREFIX")
	if imageDownloaderPrefix == "" {
		return bio_filter.Config{}, errors.New("IMAGE_DOWNLOADER_PREFIX environment variable is required")
	}

	yearFilterAmount, err := strconv.Atoi(os.Getenv("YEAR_FILTER_AMOUNT"))
	if err != nil {
		return bio_filter.Config{}, errors.New("YEAR_FILTER_AMOUNT environment variable is required and must be a number")
	}

	return bio_filter.Config{
		Id:                    id,
		MomHost:               momHost,
		MomPort:               momPort,
		BioFilterPrefix:       bioFilterPrefix,
		ImageDownloaderAmount: imageDownloaderAmount,
		ImageDownloaderPrefix: imageDownloaderPrefix,
		YearFilterAmount:      yearFilterAmount,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	worker, err := bio_filter.New(config)
	if err != nil {
		slog.Error("While initializing Bio Filter", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() { os.Exit(run()) }
