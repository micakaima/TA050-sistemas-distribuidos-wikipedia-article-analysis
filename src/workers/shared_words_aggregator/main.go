package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
	aggregator "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/shared_words_aggregator/shared_words_aggregator"
)

func loadConfig() (aggregator.SharedWordsAggregatorConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return aggregator.SharedWordsAggregatorConfig{}, fmt.Errorf(errors.ConfigSetupError, "ID")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return aggregator.SharedWordsAggregatorConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_HOST")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return aggregator.SharedWordsAggregatorConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_PORT")
	}

	sharedWordsAggregatorPrefix := os.Getenv("SHARED_WORDS_AGGREGATOR_PREFIX")
	if sharedWordsAggregatorPrefix == "" {
		return aggregator.SharedWordsAggregatorConfig{}, fmt.Errorf(errors.ConfigSetupError, "SHARED_WORDS_AGGREGATOR_PREFIX")
	}

	q3AggregatorPrefix := os.Getenv("Q3_AGGREGATOR_PREFIX")
	if q3AggregatorPrefix == "" {
		return aggregator.SharedWordsAggregatorConfig{}, fmt.Errorf(errors.ConfigSetupError, "Q3_AGGREGATOR_PREFIX")
	}

	q3AggregatorAmount, err := strconv.Atoi(os.Getenv("Q3_AGGREGATOR_AMOUNT"))
	if err != nil {
		return aggregator.SharedWordsAggregatorConfig{}, fmt.Errorf(errors.ConfigSetupError, "Q3_AGGREGATOR_AMOUNT")
	}

	vocabularyExtractorAmount := 1
	if amountStr := os.Getenv("VOCABULARY_EXTRACTOR_AMOUNT"); amountStr != "" {
		if val, err := strconv.Atoi(amountStr); err == nil && val > 0 {
			vocabularyExtractorAmount = val
		}
	}

	return aggregator.SharedWordsAggregatorConfig{
		Id:                          id,
		MomHost:                     momHost,
		MomPort:                     momPort,
		SharedWordsAggregatorPrefix: sharedWordsAggregatorPrefix,
		Q3AggregatorPrefix:          q3AggregatorPrefix,
		Q3AggregatorAmount:          q3AggregatorAmount,
		VocabularyExtractorAmount:   vocabularyExtractorAmount,
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
		slog.Error("While initializing Shared Words Aggregator", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() {
	os.Exit(run())
}
