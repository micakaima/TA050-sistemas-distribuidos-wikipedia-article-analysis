package main

import (
	"log/slog"
	"os"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/client/client"
)

func loadConfig() (client.ClientConfig, error) {
	return client.ClientConfig{
		ServerHost: "",
		ServerPort: "",
		InputFile:  "",
		OutputFile: "",
		ClientId:   0,
	}, nil
}

func run() int {
	config, err := loadConfig()
	client, err := client.NewClient(config)
	if err != nil {
		slog.Error("While creating new Client")
		return 1
	}

	if err := client.Run(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}
