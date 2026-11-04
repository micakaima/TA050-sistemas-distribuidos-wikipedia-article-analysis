package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/english_filter/english_filter"
)

func loadConfig() (english_filter.Config, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return english_filter.Config{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return english_filter.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return english_filter.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	englishFilterPrefix := os.Getenv("ENGLISH_FILTER_PREFIX")
	if englishFilterPrefix == "" {
		return english_filter.Config{}, errors.New("ENGLISH_FILTER_PREFIX environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return english_filter.Config{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	yearFilterAmount, err := strconv.Atoi(os.Getenv("YEAR_FILTER_AMOUNT"))
	if err != nil {
		return english_filter.Config{}, errors.New("YEAR_FILTER_AMOUNT environment variable is required and must be a number")
	}

	return english_filter.Config{
		Id:                  id,
		MomHost:             momHost,
		MomPort:             momPort,
		EnglishFilterPrefix: englishFilterPrefix,
		OutputQueue:         outputQueue,
		YearFilterAmount:    yearFilterAmount,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	worker, err := english_filter.New(config)
	if err != nil {
		slog.Error("While initializing English Filter", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() { os.Exit(run()) }
