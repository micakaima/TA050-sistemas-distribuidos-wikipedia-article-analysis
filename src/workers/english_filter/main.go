package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"

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

	routingKeys := strings.Split(os.Getenv("ROUTING_KEYS"), ",")
	if len(routingKeys) == 0 || routingKeys[0] == "" {
		return english_filter.Config{}, errors.New("ROUTING_KEYS environment variable is required and must be a comma-separated list")
	}

	inputExchange := os.Getenv("INPUT_EXCHANGE")
	if inputExchange == "" {
		return english_filter.Config{}, errors.New("INPUT_EXCHANGE environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return english_filter.Config{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	return english_filter.Config{
		MomHost:     momHost,
		MomPort:     momPort,
		InputExchange:  inputExchange,
		OutputQueue: outputQueue,
		RoutingKeys: routingKeys,
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