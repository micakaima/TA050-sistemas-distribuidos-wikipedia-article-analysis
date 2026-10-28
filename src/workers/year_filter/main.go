package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/year_filter/year_filter"
)

func loadConfig() (year_filter.Config, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return year_filter.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}
	
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return year_filter.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return year_filter.Config{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputExchange := os.Getenv("OUTPUT_EXCHANGE")
	if outputExchange == "" {
		return year_filter.Config{}, errors.New("OUTPUT_EXCHANGE environment variable is required")
	}

	routingKeys := strings.Split(os.Getenv("ROUTING_KEYS"), ",")
	if len(routingKeys) == 0 || routingKeys[0] == "" {
		return year_filter.Config{}, errors.New("ROUTING_KEYS environment variable is required and must be a comma-separated list")
	}

	year, err := strconv.Atoi(os.Getenv("YEAR"))
	if err != nil {
		return year_filter.Config{}, errors.New("YEAR environment variable is required and must be a number")
	}

	return year_filter.Config{
		MomHost:     momHost,
		MomPort:     momPort,
		InputQueue:  inputQueue,
		OutputExchange: outputExchange,
		RoutingKeys: routingKeys,
		Year:          year,
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

func main() {os.Exit(run())}