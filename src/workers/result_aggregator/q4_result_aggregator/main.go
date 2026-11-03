package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	aggregator "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/logic/aggregator"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	qresults "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
)

func loadConfig() (aggregator.Config[*qresults.WordFrequency], error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return aggregator.Config[*qresults.WordFrequency]{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return aggregator.Config[*qresults.WordFrequency]{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return aggregator.Config[*qresults.WordFrequency]{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return aggregator.Config[*qresults.WordFrequency]{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	storageDir := os.Getenv("STORAGE_DIR")
	if storageDir == "" {
		return aggregator.Config[*qresults.WordFrequency]{}, errors.New("STORAGE_DIR environment variable is required")
	}

	expectedEOFs := 1
	if shardsStr := os.Getenv("PARTIAL_TOP_AGGREGATOR_AMOUNT"); shardsStr != "" {
		if val, err := strconv.Atoi(shardsStr); err == nil && val > 0 {
			expectedEOFs = val
		}
	}

	return aggregator.Config[*qresults.WordFrequency]{
		MomHost:         momHost,
		MomPort:         momPort,
		InputQueue:      inputQueue,
		OutputQueue:     outputQueue,
		StorageDir:      storageDir,
		StorageName:     "q4",
		QueryCode:       qresults.Q4Code,
		ExpectedMsgCode: messages.TopWordsPartialCode,
		ExpectedEOFs:    expectedEOFs,
		Handler:         &qresults.Q4Result{},
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
