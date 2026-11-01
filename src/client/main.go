package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/client/client"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/utils"
)

func loadConfig() (client.ClientConfig, error) {
	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, fmt.Errorf(client.ConfigSetupError, "SERVER_HOST")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, fmt.Errorf(client.ConfigSetupError, "SERVER_PORT")
	}

	inputDir := os.Getenv("INPUT_DIR")
	if inputDir == "" {
		return client.ClientConfig{}, fmt.Errorf(client.ConfigSetupError, "INPUT_DIR")
	}

	outputDir := os.Getenv("OUTPUT_DIR")
	if outputDir == "" {
		return client.ClientConfig{}, fmt.Errorf(client.ConfigSetupError, "OUTPUT_DIR")
	}

	batchSizeStr := os.Getenv("BATCH_SIZE")
	if batchSizeStr == "" {
		return client.ClientConfig{}, fmt.Errorf(client.ConfigSetupError, "BATCH_SIZE")
	}
	batchSize64, err := strconv.ParseUint(batchSizeStr, 10, utils.UINT32_SIZE)
	if err != nil {
		return client.ClientConfig{}, fmt.Errorf(client.ConfigSetupError, "OUTPUT_DIR")
	}
	batchSize := uint32(batchSize64)

	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		InputDir:   inputDir,
		OutputDir:  outputDir,
		BatchSize:  batchSize,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	client, err := client.NewClient(config)
	if err != nil {
		slog.Error("While creating new Client")
		return 1
	}

	if err := client.Run(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}
