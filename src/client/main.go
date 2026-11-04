package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/client/client"
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

	batchSizeInt, err := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	if err != nil {
		return client.ClientConfig{}, fmt.Errorf(client.ConfigSetupError, "BATCH_SIZE")
	}
	batchSize := uint32(batchSizeInt)

	connectionAttempts, err := strconv.Atoi(os.Getenv("CONNECTION_ATTEMPS"))
	if err != nil {
		return client.ClientConfig{}, fmt.Errorf(client.ConfigSetupError, "CONNECTION_ATTEMPS")
	}

	connectionAttemptsDelayMs, err := strconv.Atoi(os.Getenv("CONNECTION_ATTEMPTS_DELAY_MS"))
	if err != nil {
		return client.ClientConfig{}, fmt.Errorf(client.ConfigSetupError, "CONNECTION_ATTEMPTS_DELAY_MS")
	}

	return client.ClientConfig{
		ServerHost:                serverHost,
		ServerPort:                serverPort,
		InputDir:                  inputDir,
		OutputDir:                 outputDir,
		BatchSize:                 batchSize,
		ConnectionAttempts:        connectionAttempts,
		ConnectionAttemptsDelayMs: connectionAttemptsDelayMs,
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
