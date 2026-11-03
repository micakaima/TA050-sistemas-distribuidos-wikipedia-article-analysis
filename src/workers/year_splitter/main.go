package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
	ysplitter "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/workers/year_splitter/year_splitter"
)

func loadConfig() (ysplitter.YearSplitterConfig, error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return ysplitter.YearSplitterConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_HOST")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return ysplitter.YearSplitterConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_PORT")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return ysplitter.YearSplitterConfig{}, fmt.Errorf(errors.ConfigSetupError, "INPUT_QUEUE")
	}

	oldArticlesAveragerAmount, err := strconv.Atoi(os.Getenv("OLD_ARTICLES_AVERAGER_AMOUNT"))
	if err != nil {
		return ysplitter.YearSplitterConfig{}, fmt.Errorf(errors.ConfigSetupError, "OLD_ARTICLES_AVERAGER_AMOUNT")
	}

	oldArticlesAveragerPrefix := os.Getenv("OLD_ARTICLES_AVERAGER_PREFIX")
	if oldArticlesAveragerPrefix == "" {
		return ysplitter.YearSplitterConfig{}, fmt.Errorf(errors.ConfigSetupError, "OLD_ARTICLES_AVERAGER_PREFIX")
	}

	aboveAverageFilterAmount, err := strconv.Atoi(os.Getenv("ABOVE_AVERAGE_FILTER_AMOUNT"))
	if err != nil {
		return ysplitter.YearSplitterConfig{}, fmt.Errorf(errors.ConfigSetupError, "ABOVE_AVERAGE_FILTER_AMOUNT")
	}

	aboveAverageFilterPrefix := os.Getenv("ABOVE_AVERAGE_FILTER_PREFIX_DATA")
	if aboveAverageFilterPrefix == "" {
		return ysplitter.YearSplitterConfig{}, fmt.Errorf(errors.ConfigSetupError, "ABOVE_AVERAGE_FILTER_PREFIX_DATA")
	}

	return ysplitter.YearSplitterConfig{
		MomHost:                   momHost,
		MomPort:                   momPort,
		InputQueue:                inputQueue,
		OldArticlesAveragerAmount: oldArticlesAveragerAmount,
		OldArticlesAveragerPrefix: oldArticlesAveragerPrefix,
		AboveAverageFilterAmount:  aboveAverageFilterAmount,
		AboveAverageFilterPrefix:  aboveAverageFilterPrefix,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	worker, err := ysplitter.New(config)
	if err != nil {
		slog.Error("While initializing Year Splitter", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() {
	os.Exit(run())
}
