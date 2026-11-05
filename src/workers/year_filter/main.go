package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/year_filter/year_filter"
)

func loadConfig() (year_filter.Config, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return year_filter.Config{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return year_filter.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return year_filter.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	yearFilterPrefix := os.Getenv("YEAR_FILTER_PREFIX")
	if yearFilterPrefix == "" {
		return year_filter.Config{}, errors.New("YEAR_FILTER_PREFIX environment variable is required")
	}

	englishFilterAmount, err := strconv.Atoi(os.Getenv("ENGLISH_FILTER_AMOUNT"))
	if err != nil {
		return year_filter.Config{}, errors.New("ENGLISH_FILTER_AMOUNT environment variable is required and must be a number")
	}

	englishFilterPrefix := os.Getenv("ENGLISH_FILTER_PREFIX")
	if englishFilterPrefix == "" {
		return year_filter.Config{}, errors.New("ENGLISH_FILTER_PREFIX environment variable is required")
	}

	bioFilterAmount, err := strconv.Atoi(os.Getenv("BIO_FILTER_AMOUNT"))
	if err != nil {
		return year_filter.Config{}, errors.New("BIO_FILTER_AMOUNT environment variable is required and must be a number")
	}

	bioFilterPrefix := os.Getenv("BIO_FILTER_PREFIX")
	if bioFilterPrefix == "" {
		return year_filter.Config{}, errors.New("BIO_FILTER_PREFIX environment variable is required")
	}

	year, err := strconv.Atoi(os.Getenv("YEAR"))
	if err != nil {
		return year_filter.Config{}, errors.New("YEAR environment variable is required and must be a number")
	}

	columnFilterAmount, err := strconv.Atoi(os.Getenv("COLUMN_FILTER_AMOUNT"))
	if err != nil {
		return year_filter.Config{}, fmt.Errorf("COLUMN_FILTER_AMOUNT environment variable is required and must be a number")
	}

	return year_filter.Config{
		Id:                  id,
		MomHost:             momHost,
		MomPort:             momPort,
		YearFilterPrefix:    yearFilterPrefix,
		EnglishFilterAmount: englishFilterAmount,
		EnglishFilterPrefix: englishFilterPrefix,
		BioFilterAmount:     bioFilterAmount,
		BioFilterPrefix:     bioFilterPrefix,
		Year:                year,
		ColumnFilterAmount:  columnFilterAmount,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	worker, err := year_filter.New(config)
	if err != nil {
		slog.Error("While initializing Year Filter", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() { os.Exit(run()) }
