package gateway

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	clientregistry "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/gateway/client_registry"

	external "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/external/codec"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	qresult "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
	article "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/raw_article"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type GatewayConfig struct {
	ServerHost         string
	ServerPort         string
	ColumnFilterAmount int
	ColumnFilterPrefix string
	OutputQueueName    string
	MomHost            string
	MomPort            int
	ExpectedQueries    int
}

type Gateway struct {
	registry             clientregistry.ClientRegistry
	columnFilterExchange middleware.ExchangeMiddleware
	columnFilterAmount   int
	columnFilterPrefix   string
	outputQueue          middleware.Middleware
	listener             net.Listener
	expectedQueries      int
	wg                   sync.WaitGroup
	running              atomic.Bool
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	columnFilterExchange, err := middleware.CreateExchangeMiddleware(config.ColumnFilterPrefix, []string{}, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		columnFilterExchange.Close()
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		columnFilterExchange.Close()
		outputQueue.Close()
		return nil, err
	}

	gateway := &Gateway{
		registry:             clientregistry.NewClientRegistry(),
		columnFilterExchange: columnFilterExchange,
		columnFilterAmount:   config.ColumnFilterAmount,
		columnFilterPrefix:   config.ColumnFilterPrefix,
		outputQueue:          outputQueue,
		listener:             listener,
		expectedQueries:      config.ExpectedQueries,
	}
	gateway.running.Store(true)
	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.listener.Close()
	done := make(chan struct{})
	gateway.handleSignals(done)
	defer close(done)

	utils.SpawnWithWG(&gateway.wg, func() {
		gateway.outputQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
			gateway.handleClientResponse(msg, ack, nack)
		})
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

		utils.SpawnWithWG(&gateway.wg, func() {
			gateway.handleClientRequest(client)
		})
	}

	gateway.handleShutdown()
	return nil
}

func (gateway *Gateway) handleSignals(done chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-signals:
			slog.Info("SIGTERM signal received")
			gateway.running.Store(false)
			gateway.listener.Close()
		case <-done:
			return
		}
	}()
}

func (gateway *Gateway) handleShutdown() {
	gateway.outputQueue.StopConsuming()
	gateway.registry.WithLock(func(clients map[uint64]clientregistry.ClientState) {
		for _, clientState := range clients {
			if err := clientState.Conn.Close(); err != nil {
				slog.Debug("While closing client conn")
			}
		}
	})
	gateway.wg.Wait()
	if err := gateway.columnFilterExchange.Close(); err != nil {
		slog.Debug("While closing column filter exchange")
	}
	if err := gateway.outputQueue.Close(); err != nil {
		slog.Debug("While closing output queue")
	}
}

func (gateway *Gateway) handleClientRequest(client clientregistry.ClientState) {
loop:
	for {
		msgType, err := external.ReadMsgCode(client.Conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				slog.Debug("Client disconnected gracefully")
				return
			}
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

	batchesByNode := make(map[int]article.RawArticlesBatch)
	for _, rawArt := range rawArticlesBatch.Items {
		rec := article.RawArticleToRecord(*rawArt)
		targetNode := utils.GetTargetNodeByArticleID(rec.ArticleID, gateway.columnFilterAmount)
		b, exists := batchesByNode[targetNode]
		if !exists {
			b = article.NewEmptyArticlesBatch(0)
		}
		b.Add(rawArt)
		batchesByNode[targetNode] = b
	}

	for node, batch := range batchesByNode {
		batchMessage, err := middleware.NewMessage(client.ClientID, messages.ArticlesBatchCode, &batch)
		if err != nil {
			slog.Debug("While creating Raw Articles Batch message", "err", err)
			return err
		}
		routingKey := utils.GenerateRoutingKey(gateway.columnFilterPrefix, node)
		if err := gateway.columnFilterExchange.SendToKey(*batchMessage, routingKey); err != nil {
			slog.Debug("While sending Raw Articles Batch message", "err", err)
			return err
		}
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
	for n := 0; n < gateway.columnFilterAmount; n++ {
		routingKey := utils.GenerateRoutingKey(gateway.columnFilterPrefix, n)
		if err := gateway.columnFilterExchange.SendToKey(*eofMessage, routingKey); err != nil {
			slog.Debug("While sending EOF message", "err", err)
			return err
		}
	}

	if err := external.WriteAck(client.Conn); err != nil {
		slog.Debug("While writing Ack message", "err", err)
		return err
	}
	return nil
}

func (gateway *Gateway) handleClientResponse(msg middleware.Message, ack func(), nack func()) {
	clientState, ok := gateway.registry.Get(msg.ClientID)
	if !ok {
		slog.Warn("No client is target for this message", "clientID", msg.ClientID, "msgCode", msg.MsgCode)
		ack()
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
	slog.Info("[GATEWAY] Delivering query result to client", "client", msg.ClientID, "query", queryResult.QueryCode)
	ack()
}

func (gateway *Gateway) handleResultEOF(msg middleware.Message, clientState clientregistry.ClientState, ack func(), nack func()) {
	eofs := gateway.registry.AddEOF(msg.ClientID)
	slog.Debug("Received EOF message, waiting for more results", "eofs", eofs, "expected", gateway.expectedQueries)
	if eofs >= gateway.expectedQueries {
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
	}
	ack()
}
