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
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return averager.OldArticlesAveragerConfig{}, fmt.Errorf(errors.ConfigSetupError, "ID")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return averager.OldArticlesAveragerConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_HOST")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return averager.OldArticlesAveragerConfig{}, fmt.Errorf(errors.ConfigSetupError, "MOM_PORT")
	}

	oldArticlesAveragerPrefix := os.Getenv("OLD_ARTICLES_AVERAGER_PREFIX")
	if oldArticlesAveragerPrefix == "" {
		return averager.OldArticlesAveragerConfig{}, fmt.Errorf(errors.ConfigSetupError, "OLD_ARTICLES_AVERAGER_PREFIX")
	}

	aboveAverageFilterAmount, err := strconv.Atoi(os.Getenv("ABOVE_AVERAGE_FILTER_AMOUNT"))
	if err != nil {
		return averager.OldArticlesAveragerConfig{}, fmt.Errorf(errors.ConfigSetupError, "ABOVE_AVERAGE_FILTER_AMOUNT")
	}

	aboveAverageFilterPrefix := os.Getenv("ABOVE_AVERAGE_FILTER_PREFIX_AVERAGE")
	if aboveAverageFilterPrefix == "" {
		return averager.OldArticlesAveragerConfig{}, fmt.Errorf(errors.ConfigSetupError, "ABOVE_AVERAGE_FILTER_PREFIX_AVERAGE")
	}

	return averager.OldArticlesAveragerConfig{
		Id:                        id,
		MomHost:                   momHost,
		MomPort:                   momPort,
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
