package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
	swfilter "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/stopwords_filter/stopwords_filter"
)

func loadConfig() (swfilter.StopwordsFilterConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return swfilter.StopwordsFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_HOST")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return swfilter.StopwordsFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_PORT")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return swfilter.StopwordsFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "INPUT_QUEUE")
	}

	outputQueueQ3 := os.Getenv("OUTPUT_QUEUE_Q3")
	if outputQueueQ3 == "" {
		return swfilter.StopwordsFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "OUTPUT_QUEUE_Q3")
	}

	outputQueueQ4 := os.Getenv("OUTPUT_QUEUE_Q4")
	if outputQueueQ4 == "" {
		return swfilter.StopwordsFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "OUTPUT_QUEUE_Q4")
	}

	outputQueueQ5 := os.Getenv("OUTPUT_QUEUE_Q5")
	if outputQueueQ5 == "" {
		return swfilter.StopwordsFilterConfig{}, fmt.Errorf(errors.ConfigSetupError, "OUTPUT_QUEUE_Q5")
	}

	return swfilter.StopwordsFilterConfig{
		MomHost:       momHost,
		MomPort:       momPort,
		InputQueue:    inputQueue,
		OutputQueueQ3: outputQueueQ3,
		OutputQueueQ4: outputQueueQ4,
		OutputQueueQ5: outputQueueQ5,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	worker, err := swfilter.New(config)
	if err != nil {
		slog.Error("While initializing Stop Words Filter", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() {
	os.Exit(run())
}
