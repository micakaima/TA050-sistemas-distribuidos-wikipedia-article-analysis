package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
	averager "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/old_articles_averager/old_articles_averager"
)

func loadConfig() (averager.OldArticlesAveragerConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return averager.OldArticlesAveragerConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_HOST")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return averager.OldArticlesAveragerConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_PORT")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return averager.OldArticlesAveragerConfig{}, fmt.Errorf(errors.ConfigSetupError, "INPUT_QUEUE")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return averager.OldArticlesAveragerConfig{}, fmt.Errorf(errors.ConfigSetupError, "OUTPUT_QUEUE")
	}

	return averager.OldArticlesAveragerConfig{
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
	worker, err := averager.New(config)
	if err != nil {
		slog.Error("While initializing Old Articles Averager", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() {
	os.Exit(run())
}
