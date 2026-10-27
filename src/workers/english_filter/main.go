package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"src/workers/english_filter/english_filter"
)

func loadConfig() (english_filter.Config, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return english_filter.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}
	
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return english_filter.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return english_filter.Config{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return english_filter.Config{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	return english_filter.Config{
		MomHost:     momHost,
		MomPort:     momPort,
		InputQueue:  inputQueue,
		OutputQueue: outputQueue,
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

func main() {os.Exit(run())}