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
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_HOST")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_PORT")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "INPUT_QUEUE")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return vextractor.VocabularyExtractorConfig{}, fmt.Errorf(errors.ConfigSetupError, "OUTPUT_QUEUE")
	}

	return vextractor.VocabularyExtractorConfig{
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
