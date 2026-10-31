package gateway

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	clientregistry "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/gateway/client_registry"

	external "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/codec"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	qresult "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
)

type GatewayConfig struct {
	ServerHost      string
	ServerPort      string
	InputQueueName  string
	OutputQueueName string
	MomHost         string
	MomPort         int
}

type Gateway struct {
	registry    clientregistry.ClientRegistry
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
	listener    net.Listener
	running     atomic.Bool
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, err
	}

	gateway := &Gateway{
		registry:    clientregistry.NewClientRegistry(),
		inputQueue:  inputQueue,
		outputQueue: outputQueue,
		listener:    listener,
	}
	gateway.running.Store(true)
	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.listener.Close()
	gateway.handleSignals()

	go gateway.outputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		gateway.handleClientResponse(msg, ack, nack)
	})

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

	gateway.outputQueue.StopConsuming()
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
	go func() {
		<-signals
		slog.Info("SIGTERM signal received")
		gateway.running.Store(false)
		gateway.listener.Close()
	}()
}

func (gateway *Gateway) handleClientRequest(client clientregistry.ClientState) {
loop:
	for {
		msgType, err := external.ReadMsgCode(client.Conn)
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
	rawArticlesBatch, err := external.ReadArticlesBatch(client.Conn)
	if err != nil {
		slog.Debug("While reading Articles Batch", "err", err)
		return err
	}

	batchMessage, err := middleware.NewMessage(client.ClientID, messages.ArticlesBatchCode, &rawArticlesBatch)
	if err != nil {
		slog.Debug("While creating Raw Articles Batch message", "err", err)
		return err
	}
	if err := gateway.inputQueue.Send(*batchMessage); err != nil {
		slog.Debug("While sending Raw Articles Batch message", "err", err)
		return err
	}

	if err := external.WriteAck(client.Conn); err != nil {
		slog.Debug("While writing Ack message", "err", err)
		return err
	}
	return nil
}

func (gateway *Gateway) handleEOF(client clientregistry.ClientState) error {
	slog.Info("Received END_OF_RECORDS message")

	eof := messages.EofMsg{}
	eofMessage, err := middleware.NewMessage(client.ClientID, messages.EOFCode, &eof)
	if err != nil {
		slog.Debug("While creating EOF message", "err", err)
		return err
	}
	if err := gateway.inputQueue.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	if err := external.WriteAck(client.Conn); err != nil {
		slog.Debug("While writing Ack message", "err", err)
		return err
	}
	return nil
}

func (gateway *Gateway) handleClientResponse(msg middleware.Message, ack func(), nack func()) {
	gateway.registry.WithLock(func(clients map[uint64]clientregistry.ClientState) {
		clientState, ok := clients[msg.ClientID]
		if !ok {
			slog.Warn("No client is target for this message")
			nack()
			return
		}
		switch msg.MsgCode {
		case messages.ResultCode:
			gateway.handleQueryResult(msg, clientState, ack, nack)

		case messages.EOFCode:
			gateway.handleResultEOF(msg, clientState, ack, nack)

		default:
			slog.Debug("Received unexpected inner message code")
			nack()
		}
	})
}

func (gateway *Gateway) handleQueryResult(msg middleware.Message, clientState clientregistry.ClientState, ack func(), nack func()) {
	queryResult, err := qresult.QueryResultFromBytes([]byte(msg.Body))
	if err != nil {
		slog.Debug("While reading from output queue", "err", err)
		nack()
		gateway.outputQueue.StopConsuming()
		return
	}
	if err := external.WriteQueryResult(clientState.Conn, *queryResult); err != nil {
		slog.Debug("While writing QueryResult message", "err", err)
		return
	}
	ack()
}

func (gateway *Gateway) handleResultEOF(msg middleware.Message, clientState clientregistry.ClientState, ack func(), nack func()) {
	if err := external.WriteEOF(clientState.Conn); err != nil {
		slog.Debug("While writing EOF message", "err", err)
		return
	}
	msgType, err := external.ReadMsgCode(clientState.Conn)
	if err != nil {
		slog.Debug("While reading message type", "err", err)
		return
	}
	if msgType != external.Ack {
		slog.Debug("Unexpected message type", "err", err)
		return
	}
	gateway.registry.Remove(msg.ClientID)
	ack()
}
