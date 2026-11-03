package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	aggregator "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/result_aggregator/result_aggregator"
)

func loadConfig() (aggregator.Config, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return aggregator.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return aggregator.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	q1InputQueue := os.Getenv("Q1_INPUT_QUEUE")
	if q1InputQueue == "" {
		return aggregator.Config{}, errors.New("Q1_INPUT_QUEUE environment variable is required")
	}

	q2InputQueue := os.Getenv("Q2_INPUT_QUEUE")
	if q2InputQueue == "" {
		return aggregator.Config{}, errors.New("Q2_INPUT_QUEUE environment variable is required")
	}

	q3EofInputQueue := os.Getenv("Q3_EOF_INPUT_QUEUE")
	if q3EofInputQueue == "" {
		return aggregator.Config{}, errors.New("Q3_EOF_INPUT_QUEUE environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return aggregator.Config{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	storageDir := os.Getenv("STORAGE_DIR")
	if storageDir == "" {
		return aggregator.Config{}, errors.New("STORAGE_DIR environment variable is required")
	}

	return aggregator.Config{
		MomHost:         momHost,
		MomPort:         momPort,
		Q1InputQueue:    q1InputQueue,
		Q2InputQueue:    q2InputQueue,
		Q3EOFInputQueue: q3EofInputQueue,
		OutputQueue:     outputQueue,
		StorageDir:      storageDir,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	worker, err := aggregator.New(config)
	if err != nil {
		slog.Error("While initializing Result Aggregator", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() { os.Exit(run()) }
