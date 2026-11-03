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
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return abovefilter.AboveAverageFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "ID")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return abovefilter.AboveAverageFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_HOST")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return abovefilter.AboveAverageFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_PORT")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return abovefilter.AboveAverageFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "OUTPUT_QUEUE")
	}

	aboveAverageFilterPrefixData := os.Getenv("ABOVE_AVERAGE_FILTER_PREFIX_DATA")
	if aboveAverageFilterPrefixData == "" {
		return abovefilter.AboveAverageFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "ABOVE_AVERAGE_FILTER_PREFIX_DATA")
	}

	aboveAverageFilterPrefixAverage := os.Getenv("ABOVE_AVERAGE_FILTER_PREFIX_AVERAGE")
	if aboveAverageFilterPrefixAverage == "" {
		return abovefilter.AboveAverageFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "ABOVE_AVERAGE_FILTER_PREFIX_AVERAGE")
	}

	return abovefilter.AboveAverageFilterConfig{
		Id:                              id,
		MomHost:                         momHost,
		MomPort:                         momPort,
		OutputQueue:                     outputQueue,
		AboveAverageFilterPrefixData:    aboveAverageFilterPrefixData,
		AboveAverageFilterPrefixAverage: aboveAverageFilterPrefixAverage,
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
