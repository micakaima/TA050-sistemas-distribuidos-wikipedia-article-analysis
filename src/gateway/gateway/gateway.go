package gateway

import (
	clientregistry "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/gateway/client_registry"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	external "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external"
)

type GatewayConfig struct {
	ServerHost string
	ServerPort string
}

type Gateway struct {
	registry clientregistry.ClientRegistry
	listener net.Listener
	running  atomic.Bool
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		return nil, err
	}

	gateway := &Gateway{
		listener: listener,
		registry: clientregistry.NewClientRegistry(),
	}
	gateway.running.Store(true)
	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.listener.Close()
	go gateway.handleSignals()

	for {
		conn, err := gateway.listener.Accept()
		if err != nil {
			if !gateway.running.Load() {
				break
			}
			return err
		}
		slog.Info("Client connected...")

		client := clientregistry.NewClientState(conn)
		gateway.registry.Add(client)

		go gateway.handleClientRequest(client)
	}

	gateway.registry.WithLock(func(clients map[uint64]clientregistry.ClientState) {
		for _, clientState := range clients {
			clientState.Conn.Close()
		}
	})

	return nil
}

func (gateway *Gateway) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	gateway.running.Store(false)
	gateway.listener.Close()
}

func (gateway *Gateway) handleClientRequest(client clientregistry.ClientState) {
loop:
	for {
		msgType, err := external.ReadMsgType(client.Conn)
		if err != nil {
			slog.Error("While reading message type", "err", err)
			return
		}

		switch msgType {
		case external.ArticlesBatch:
			if err := gateway.handleArticlesBatch(client); err != nil {
				slog.Error("While handling Articles Batch", "err", err)
				return
			}
		case external.EOF:
			if err := gateway.handleEOF(client); err != nil {
				slog.Error("While handling EOF message", "err", err)
				return
			}
			break loop
		default:
			slog.Error("Read unexpected message type")
			return
		}
	}
}

func (gateway *Gateway) handleArticlesBatch(client clientregistry.ClientState) error {
	_, err := external.ReadArticlesBatch(client.Conn) // rawArticlesBatch
	if err != nil {
		slog.Error("While reading Articles Batch", "err", err)
		return err
	}

	// Aca se deberia enviar rawArticlesBatch a la siguiente etapa (columnFilter)

	if err := external.WriteAck(client.Conn); err != nil {
		slog.Error("While writing Ack message", "err", err)
		return err
	}
	return nil
}

func (gateway *Gateway) handleEOF(client clientregistry.ClientState) error {
	slog.Info("Received END_OF_RECORDS message")
	if err := external.WriteAck(client.Conn); err != nil {
		slog.Error("While writing Ack message", "err", err)
		return err
	}
	return nil
}

// func (gateway *Gateway) handleClientResponse()
