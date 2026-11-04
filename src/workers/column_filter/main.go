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

	outQ1ToQ2 := os.Getenv("OUTPUT_QUEUE_Q1_TO_Q2")
	if outQ1ToQ2 == "" {
		return column_filter.Config{}, errors.New("OUTPUT_QUEUE_Q1_TO_Q2 environment variable is required")
	}

	outQ3ToQ5 := os.Getenv("OUTPUT_QUEUE_Q3_TO_Q5")
	if outQ3ToQ5 == "" {
		return column_filter.Config{}, errors.New("OUTPUT_QUEUE_Q3_TO_Q5 environment variable is required")
	}

	columnFilterPrefix := os.Getenv("COLUMN_FILTER_PREFIX")
	if columnFilterPrefix == "" {
		return column_filter.Config{}, errors.New("COLUMN_FILTER_PREFIX environment variable is required")
	}

	return column_filter.Config{
		Id:                 id,
		MomHost:            momHost,
		MomPort:            momPort,
		OutputQ1ToQ2:       outQ1ToQ2,
		OutputQ3ToQ5:       outQ3ToQ5,
		ColumnFilterPrefix: columnFilterPrefix,
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
