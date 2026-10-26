package main

import (
	"fmt"
	"log/slog"
	"os"

	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/gateway/gateway"
)

func loadConfig() (gateway.GatewayConfig, error) {
	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return gateway.GatewayConfig{}, fmt.Errorf(gateway.ConfigSetupError, "SERVER_HOST")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return gateway.GatewayConfig{}, fmt.Errorf(gateway.ConfigSetupError, "SERVER_PORT")
	}

	return gateway.GatewayConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
	}, nil
}

func run() int {
	config, err := loadConfig()
	gateway, err := gateway.NewGateway(config)
	if err != nil {
		slog.Error("While creating new Gateway")
		return 1
	}

	if err := gateway.Run(); err != nil {
		slog.Error("Gateway stopped with error", "err", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}
