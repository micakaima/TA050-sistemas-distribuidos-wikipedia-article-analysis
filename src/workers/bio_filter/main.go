package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"src/workers/bio_filter/bio_filter"
)

func loadConfig() (bio_filter.Config, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return bio_filter.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}
	
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return bio_filter.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return bio_filter.Config{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return bio_filter.Config{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	return bio_filter.Config{
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
	worker, err := bio_filter.New(config)
	if err != nil {
		slog.Error("While initializing Bio Filter", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() {os.Exit(run())}