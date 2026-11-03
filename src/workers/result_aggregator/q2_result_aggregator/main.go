package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	aggregator "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/logic/aggregator"
	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	qresults "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
)

func buildQ2Result(batch generic.Batch[*qresults.ImageArticle]) *qresults.QueryResult {
	payload, err := qresults.Q2Result{ResultArticles: batch}.ToBytes()
	if err != nil {
		slog.Error("While serializing Q2Result", "err", err)
		return nil
	}
	return &qresults.QueryResult{
		QueryCode:     qresults.Q2Code,
		ResultPayload: payload,
	}
}

func logic() aggregator.Logic[*qresults.ImageArticle] {
	return aggregator.Logic[*qresults.ImageArticle]{
		DeserializeQueryResult: func(data []byte) (generic.Batch[*qresults.ImageArticle], error) {
			res, err := qresults.Q2ResultFromBytes(data)
			if err != nil {
				return generic.Batch[*qresults.ImageArticle]{}, err
			}
			return res.ResultArticles, nil
		},

		NewEmptyBatch:           qresults.NewEmptyImageArticleBatch,
		DeserializeArticleBatch: qresults.ImageArticleBatchFromBytes,
		BuildQueryResult:        buildQ2Result,
	}
}
func loadConfig() (aggregator.Config[*qresults.ImageArticle], error) {
	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return aggregator.Config[*qresults.ImageArticle]{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return aggregator.Config[*qresults.ImageArticle]{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return aggregator.Config[*qresults.ImageArticle]{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return aggregator.Config[*qresults.ImageArticle]{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	storageDir := os.Getenv("STORAGE_DIR")
	if storageDir == "" {
		return aggregator.Config[*qresults.ImageArticle]{}, errors.New("STORAGE_DIR environment variable is required")
	}

	return aggregator.Config[*qresults.ImageArticle]{
		MomHost:         momHost,
		MomPort:         momPort,
		InputQueue:      inputQueue,
		OutputQueue:     outputQueue,
		StorageDir:      storageDir,
		StorageName:     "q2",
		QueryCode:       qresults.Q2Code,
		ExpectedMsgCode: messages.Q2ResultCode,
		Logic:           logic(),
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
		slog.Error("While initializing Result Aggregator", "err", err)
		return 1
	}
	worker.Run()
	return 0
}

func main() { os.Exit(run()) }
