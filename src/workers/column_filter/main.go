package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/column_filter/column_filter"
)

func loadConfig() (column_filter.Config, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return column_filter.Config{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return column_filter.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return column_filter.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	columnFilterPrefix := os.Getenv("COLUMN_FILTER_PREFIX")
	if columnFilterPrefix == "" {
		return column_filter.Config{}, errors.New("COLUMN_FILTER_PREFIX environment variable is required")
	}

	yearFilterAmount, err := strconv.Atoi(os.Getenv("YEAR_FILTER_AMOUNT"))
	if err != nil {
		return column_filter.Config{}, errors.New("YEAR_FILTER_AMOUNT environment variable is required and must be a number")
	}

	yearFilterPrefix := os.Getenv("YEAR_FILTER_PREFIX")
	if yearFilterPrefix == "" {
		return column_filter.Config{}, errors.New("YEAR_FILTER_PREFIX environment variable is required")
	}

	vocabularyExtractorAmount, err := strconv.Atoi(os.Getenv("VOCABULARY_EXTRACTOR_AMOUNT"))
	if err != nil {
		return column_filter.Config{}, errors.New("VOCABULARY_EXTRACTOR_AMOUNT environment variable is required and must be a number")
	}

	vocabularyExtractorPrefix := os.Getenv("VOCABULARY_EXTRACTOR_PREFIX")
	if vocabularyExtractorPrefix == "" {
		return column_filter.Config{}, errors.New("VOCABULARY_EXTRACTOR_PREFIX environment variable is required")
	}

	return column_filter.Config{
		Id:                        id,
		MomHost:                   momHost,
		MomPort:                   momPort,
		ColumnFilterPrefix:        columnFilterPrefix,
		YearFilterAmount:          yearFilterAmount,
		YearFilterPrefix:          yearFilterPrefix,
		VocabularyExtractorAmount: vocabularyExtractorAmount,
		VocabularyExtractorPrefix: vocabularyExtractorPrefix,
	}, nil

}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	worker, err := column_filter.New(config)
	if err != nil {
		slog.Error("While initializing Column Filter", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() { os.Exit(run()) }
