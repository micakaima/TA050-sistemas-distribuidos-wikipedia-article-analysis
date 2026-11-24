package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
	balancer "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/load_balancer/load_balancer"
)

func loadConfig() (balancer.LoadBalancerConfig, error) {
	loadBalancerHost := os.Getenv("LOAD_BALANCER_HOST")
	if loadBalancerHost == "" {
		return balancer.LoadBalancerConfig{}, fmt.Errorf(errors.ConfigSetupError, "LOAD_BALANCER_HOST")
	}

	loadBalancerPort := os.Getenv("LOAD_BALANCER_PORT")
	if loadBalancerPort == "" {
		return balancer.LoadBalancerConfig{}, fmt.Errorf(errors.ConfigSetupError, "LOAD_BALANCER_PORT")
	}

	baseServerHost := os.Getenv("BASE_SERVER_HOST")
	if baseServerHost == "" {
		return balancer.LoadBalancerConfig{}, fmt.Errorf(errors.ConfigSetupError, "BASE_SERVER_HOST")
	}

	baseServerPort := os.Getenv("BASE_SERVER_PORT")
	if baseServerPort == "" {
		return balancer.LoadBalancerConfig{}, fmt.Errorf(errors.ConfigSetupError, "BASE_SERVER_PORT")
	}

	gatewayAmount, err := strconv.Atoi(os.Getenv("GATEWAY_AMOUNT"))
	if err != nil {
		return balancer.LoadBalancerConfig{}, fmt.Errorf(errors.ConfigSetupError, "GATEWAY_AMOUNT")
	}

	return balancer.LoadBalancerConfig{
		LoadBalancerHost: loadBalancerHost,
		LoadBalancerPort: loadBalancerPort,
		BaseServerHost:   baseServerHost,
		BaseServerPort:   baseServerPort,
		GatewayAmount:    gatewayAmount,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}
	loadBalancer, err := balancer.NewLoadBalancer(config)
	if err != nil {
		slog.Error("While creating new Load Balancer")
		return 1
	}

	if err := loadBalancer.Run(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}
