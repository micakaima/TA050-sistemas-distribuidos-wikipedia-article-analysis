package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
	aggregator "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/partial_top_aggregator/partial_top_aggregator"
)

func loadConfig() (aggregator.PartialTopAggregatorConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return aggregator.PartialTopAggregatorConfig{}, fmt.Errorf(errors.ConfigSetupError, "ID")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return aggregator.PartialTopAggregatorConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_HOST")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return aggregator.PartialTopAggregatorConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_PORT")
	}

	partialTopAggregatorPrefix := os.Getenv("PARTIAL_TOP_AGGREGATOR_PREFIX")
	if partialTopAggregatorPrefix == "" {
		return aggregator.PartialTopAggregatorConfig{}, fmt.Errorf(errors.ConfigSetupError, "PARTIAL_TOP_AGGREGATOR_PREFIX")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return aggregator.PartialTopAggregatorConfig{}, fmt.Errorf(errors.ConfigSetupError, "OUTPUT_QUEUE")
	}

	return aggregator.PartialTopAggregatorConfig{
		Id:                         id,
		MomHost:                    momHost,
		MomPort:                    momPort,
		PartialTopAggregatorPrefix: partialTopAggregatorPrefix,
		OutputQueue:                outputQueue,
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
		slog.Error("While initializing Partial Top Aggregator", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() {
	os.Exit(run())
}
