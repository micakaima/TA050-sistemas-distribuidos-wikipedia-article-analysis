package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
	joiner "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/shared_words_joiner/shared_words_joiner"
)

func loadConfig() (joiner.SharedWordsJoinerConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return joiner.SharedWordsJoinerConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_HOST")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return joiner.SharedWordsJoinerConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_PORT")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return joiner.SharedWordsJoinerConfig{}, fmt.Errorf(errors.ConfigSetupError, "INPUT_QUEUE")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return joiner.SharedWordsJoinerConfig{}, fmt.Errorf(errors.ConfigSetupError, "OUTPUT_QUEUE")
	}

	q3EofOutputQueue := os.Getenv("Q3_EOF_OUTPUT_QUEUE")
	if q3EofOutputQueue == "" {
		return joiner.SharedWordsJoinerConfig{}, fmt.Errorf(errors.ConfigSetupError, "Q3_EOF_OUTPUT_QUEUE")
	}

	sharedWordsAggregatorAmount, err := strconv.Atoi(os.Getenv("SHARED_WORDS_AGGREGATOR_AMOUNT"))
	if err != nil {
		return joiner.SharedWordsJoinerConfig{}, fmt.Errorf(errors.ConfigSetupError, "SHARED_WORDS_AGGREGATOR_AMOUNT")
	}

	return joiner.SharedWordsJoinerConfig{
		MomHost:                     momHost,
		MomPort:                     momPort,
		InputQueue:                  inputQueue,
		OutputQueue:                 outputQueue,
		Q3EofOutputQueue:            q3EofOutputQueue,
		SharedWordsAggregatorAmount: sharedWordsAggregatorAmount,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	worker, err := joiner.New(config)
	if err != nil {
		slog.Error("While initializing Shared Words Joiner", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() {
	os.Exit(run())
}
