package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

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

	columnFilterAmount, err := strconv.Atoi(os.Getenv("COLUMN_FILTER_AMOUNT"))
	if err != nil {
		return gateway.GatewayConfig{}, fmt.Errorf(gateway.ConfigSetupError, "COLUMN_FILTER_AMOUNT")
	}

	columnFilterPrefix := os.Getenv("COLUMN_FILTER_PREFIX")
	if columnFilterPrefix == "" {
		return gateway.GatewayConfig{}, fmt.Errorf(gateway.ConfigSetupError, "COLUMN_FILTER_PREFIX")
	}

	outputQueueName := os.Getenv("OUTPUT_QUEUE")
	if outputQueueName == "" {
		return gateway.GatewayConfig{}, fmt.Errorf(gateway.ConfigSetupError, "OUTPUT_QUEUE")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return gateway.GatewayConfig{}, fmt.Errorf(gateway.ConfigSetupError, "MOM_HOST")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return gateway.GatewayConfig{}, fmt.Errorf(gateway.ConfigSetupError, "MOM_PORT")
	}

	return gateway.GatewayConfig{
		ServerHost:         serverHost,
		ServerPort:         serverPort,
		ColumnFilterAmount: columnFilterAmount,
		ColumnFilterPrefix: columnFilterPrefix,
		OutputQueueName:    outputQueueName,
		MomHost:            momHost,
		MomPort:            momPort,
		ExpectedQueries:    5,
	}, nil

}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
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
