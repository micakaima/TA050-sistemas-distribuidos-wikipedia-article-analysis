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

	id := 0
	if idStr := os.Getenv("ID"); idStr != "" {
		if val, err := strconv.Atoi(idStr); err == nil {
			id = val
		}
	}

	inputExchangePrefix := os.Getenv("INPUT_EXCHANGE_PREFIX")
	if inputExchangePrefix == "" {
		return aggregator.Config[*qresults.WordFrequency]{}, errors.New("INPUT_EXCHANGE_PREFIX environment variable is required")
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

	gatewayPrefix := os.Getenv("GATEWAY_PREFIX")
	if gatewayPrefix == "" {
		return aggregator.Config[*qresults.WordFrequency]{}, errors.New("GATEWAY_PREFIX environment variable is required")
	}

	return aggregator.Config[*qresults.WordFrequency]{
		Id:                  id,
		MomHost:             momHost,
		MomPort:             momPort,
		InputExchangePrefix: inputExchangePrefix,
		StorageDir:          storageDir,
		StorageName:         "q4",
		QueryCode:           qresults.Q4Code,
		ExpectedMsgCode:     messages.TopWordsPartialCode,
		ExpectedEOFs:        expectedEOFs,
		GatewayPrefix:       gatewayPrefix,
		Handler:             &qresults.Q4Result{},
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
