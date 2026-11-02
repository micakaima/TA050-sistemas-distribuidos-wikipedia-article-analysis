package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
	abovefilter "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/above_average_filter/above_average_filter"
)

func loadConfig() (abovefilter.AboveAverageFilterConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return abovefilter.AboveAverageFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_HOST")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return abovefilter.AboveAverageFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_PORT")
	}

	dataInputQueue := os.Getenv("INPUT_QUEUE_DATA")
	if dataInputQueue == "" {
		return abovefilter.AboveAverageFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "INPUT_QUEUE_DATA")
	}

	averageInputQueue := os.Getenv("INPUT_QUEUE_AVERAGE")
	if averageInputQueue == "" {
		return abovefilter.AboveAverageFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "INPUT_QUEUE_AVERAGE")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return abovefilter.AboveAverageFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "OUTPUT_QUEUE")
	}

	return abovefilter.AboveAverageFilterConfig{
		MomHost:           momHost,
		MomPort:           momPort,
		DataInputQueue:    dataInputQueue,
		AverageInputQueue: averageInputQueue,
		OutputQueue:       outputQueue,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	worker, err := abovefilter.New(config)
	if err != nil {
		slog.Error("While initializing Above Average Filter", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() {
	os.Exit(run())
}
