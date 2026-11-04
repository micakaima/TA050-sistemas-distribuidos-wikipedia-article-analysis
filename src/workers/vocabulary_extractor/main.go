package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
	vextractor "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/vocabulary_extractor/vocabulary_extractor"
)

func loadConfig() (vextractor.VocabularyExtractorConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "ID")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_HOST")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_PORT")
	}

	vocabularyExtractorPrefix := os.Getenv("VOCABULARY_EXTRACTOR_PREFIX")
	if vocabularyExtractorPrefix == "" {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "VOCABULARY_EXTRACTOR_PREFIX")
	}

	outputQueueQ5 := os.Getenv("OUTPUT_QUEUE_Q5")
	if outputQueueQ5 == "" {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "OUTPUT_QUEUE_Q5")
	}

	sharedWordsAggregatorAmount, err := strconv.Atoi(os.Getenv("SHARED_WORDS_AGGREGATOR_AMOUNT"))
	if err != nil {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "SHARED_WORDS_AGGREGATOR_AMOUNT")
	}

	sharedWordsAggregatorPrefix := os.Getenv("SHARED_WORDS_AGGREGATOR_PREFIX")
	if sharedWordsAggregatorPrefix == "" {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "SHARED_WORDS_AGGREGATOR_PREFIX")
	}

	partialTopAggregatorAmount, err := strconv.Atoi(os.Getenv("PARTIAL_TOP_AGGREGATOR_AMOUNT"))
	if err != nil {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "PARTIAL_TOP_AGGREGATOR_AMOUNT")
	}

	partialTopAggregatorPrefix := os.Getenv("PARTIAL_TOP_AGGREGATOR_PREFIX")
	if partialTopAggregatorPrefix == "" {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "PARTIAL_TOP_AGGREGATOR_PREFIX")
	}

	columnFilterAmount, err := strconv.Atoi(os.Getenv("COLUMN_FILTER_AMOUNT"))
	if err != nil {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "COLUMN_FILTER_AMOUNT")
	}

	return vextractor.VocabularyExtractorConfig{
		Id:                          id,
		MomHost:                     momHost,
		MomPort:                     momPort,
		VocabularyExtractorPrefix:   vocabularyExtractorPrefix,
		OutputQueueQ5:               outputQueueQ5,
		SharedWordsAggregatorAmount: sharedWordsAggregatorAmount,
		SharedWordsAggregatorPrefix: sharedWordsAggregatorPrefix,
		PartialTopAggregatorAmount:  partialTopAggregatorAmount,
		PartialTopAggregatorPrefix:  partialTopAggregatorPrefix,
		ColumnFilterAmount:          columnFilterAmount,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	worker, err := vextractor.New(config)
	if err != nil {
		slog.Error("While initializing Vocabulary Extractor", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() {
	os.Exit(run())
}
