package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	manager "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/managers/column_filter_manager/column_filter_manager"
)

func loadConfig() (manager.ColumnFilterManagerConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return manager.ColumnFilterManagerConfig{}, errors.New("ID environment variable is required and must be a number")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return manager.ColumnFilterManagerConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return manager.ColumnFilterManagerConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return manager.ColumnFilterManagerConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	columnFilterAmount, err := strconv.Atoi(os.Getenv("COLUMN_FILTER_AMOUNT"))
	if err != nil {
		return manager.ColumnFilterManagerConfig{}, errors.New("COLUMN_FILTER_AMOUNT environment variable is required and must be a number")
	}

	columnFilterPrefix := os.Getenv("COLUMN_FILTER_PREFIX")
	if columnFilterPrefix == "" {
		return manager.ColumnFilterManagerConfig{}, errors.New("COLUMN_FILTER_PREFIX environment variable is required")
	}

	return manager.ColumnFilterManagerConfig{
		Id:                 id,
		MomHost:            momHost,
		MomPort:            momPort,
		InputQueue:         inputQueue,
		ColumnFilterAmount: columnFilterAmount,
		ColumnFilterPrefix: columnFilterPrefix,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	worker, err := manager.NewColumnFilterManager(config)
	if err != nil {
		slog.Error("While initializing Column Filter Manager", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() { os.Exit(run()) }
