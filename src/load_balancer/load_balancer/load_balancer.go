package loadbalancer

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

	external "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/codec"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type LoadBalancerConfig struct {
	LoadBalancerHost string
	LoadBalancerPort string
	BaseServerHost   string
	BaseServerPort   string
	GatewayAmount    int
}

type LoadBalancer struct {
	listener       net.Listener
	gatewayAmount  int
	baseServerHost string
	baseServerPort int
	nextIndex      int
	wg             sync.WaitGroup
	mutex          sync.Mutex
	running        atomic.Bool
}

func NewLoadBalancer(config LoadBalancerConfig) (*LoadBalancer, error) {
	baseServerPort, err := strconv.Atoi(config.BaseServerPort)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", config.LoadBalancerHost+":"+config.LoadBalancerPort)
	if err != nil {
		return nil, err
	}

	loadBalancer := &LoadBalancer{
		listener:       listener,
		gatewayAmount:  config.GatewayAmount,
		baseServerHost: config.BaseServerHost,
		baseServerPort: baseServerPort,
		nextIndex:      0,
		mutex:          sync.Mutex{},
	}
	loadBalancer.running.Store(true)
	return loadBalancer, nil
}

func (balancer *LoadBalancer) Run() error {
	defer balancer.listener.Close()
	done := make(chan struct{})
	balancer.handleSignals(done)
	defer close(done)

	for {
		conn, err := balancer.listener.Accept()
		if err != nil {
			if !balancer.running.Load() {
				break
			}
			return err
		}
		slog.Info("Client connected...")

		utils.SpawnWithWG(&balancer.wg, func() {
			balancer.handleClientRequest(conn)
		})
	}

	balancer.wg.Wait()
	return nil
}

func (balancer *LoadBalancer) handleSignals(done chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-signals:
			slog.Info("SIGTERM signal received")
			balancer.running.Store(false)
			balancer.listener.Close()
		case <-done:
			return
		}
	}()
}

func (balancer *LoadBalancer) handleClientRequest(clientConn net.Conn) {
	serverHost, serverPort := balancer.pickGateway()
	if err := external.WriteRedirect(clientConn, serverHost, serverPort); err != nil {
		slog.Debug("While writing Redirect message", "err", err)
		return
	}
	slog.Info(fmt.Sprintf("Client redirected to %s", serverHost))
	clientConn.Close()
}

func (balancer *LoadBalancer) pickGateway() (string, int) {
	pickedIndex := 0
	utils.WithLock(&balancer.mutex, func() {
		pickedIndex = (balancer.nextIndex) % balancer.gatewayAmount
		balancer.nextIndex++
	})

	serverHost := fmt.Sprintf("%s_%d", balancer.baseServerHost, pickedIndex)
	serverPort := balancer.baseServerPort
	return serverHost, serverPort
}
