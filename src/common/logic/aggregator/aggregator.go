package aggregator

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	qresults "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type Config[T generic.SerializableMessage] struct {
	Id                  int
	MomHost             string
	MomPort             int
	InputExchangePrefix string
	StorageDir          string
	StorageName         string
	QueryCode           qresults.QueryResultCode
	ExpectedMsgCode     messages.InnerMsgCode
	ExpectedEOFs        int
	Handler             qresults.QueryResultHandler[T]
	GatewayPrefix       string
}

type Aggregator[T generic.SerializableMessage] struct {
	id              int
	inputExchange   middleware.Middleware
	outputExchange  middleware.ExchangeMiddleware
	storage         *Storage
	queryCode       qresults.QueryResultCode
	expectedMsgCode messages.InnerMsgCode
	expectedEOFs    int
	handler         qresults.QueryResultHandler[T]
	gatewayPrefix   string

	mu           sync.Mutex
	eofsReceived map[uint64]int
	clientDone   map[uint64]bool
}

func New[T generic.SerializableMessage](config Config[T]) (*Aggregator[T], error) {
	settings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputRoutingKey := []string{utils.GenerateRoutingKey(config.InputExchangePrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchangePrefix, inputRoutingKey, settings)
	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.GatewayPrefix, []string{}, settings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	expectedEOFs := config.ExpectedEOFs
	if expectedEOFs <= 0 {
		expectedEOFs = 1
	}

	return &Aggregator[T]{
		id:              config.Id,
		inputExchange:   inputExchange,
		outputExchange:  outputExchange,
		storage:         NewStorage(config.StorageName, config.StorageDir),
		queryCode:       config.QueryCode,
		expectedMsgCode: config.ExpectedMsgCode,
		expectedEOFs:    expectedEOFs,
		handler:         config.Handler,
		gatewayPrefix:   config.GatewayPrefix,
		eofsReceived:    make(map[uint64]int),
		clientDone:      make(map[uint64]bool),
	}, nil
}

func (w *Aggregator[T]) Run() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	var waitGroup sync.WaitGroup
	utils.SpawnWithWG(&waitGroup, func() {
		if err := w.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			w.handleMessage(msg, ack, nack)
		}); err != nil {
			slog.Error("Consumer stopped", "err", err)
		}
	})

	<-signals
	w.inputExchange.StopConsuming()
	waitGroup.Wait()
	w.shutdown()
}

func (w *Aggregator[T]) shutdown() {
	if err := w.inputExchange.Close(); err != nil {
		slog.Error("While closing input exchange", "err", err)
	}
	if err := w.outputExchange.Close(); err != nil {
		slog.Error("While closing output exchange", "err", err)
	}
}

func (w *Aggregator[T]) handleMessage(msg middleware.Message, ack, nack func()) {
	if msg.MsgCode == messages.EOFCode {
		shouldFinish := utils.WithLockAndReturn(&w.mu, func() bool {
			w.eofsReceived[msg.ClientID]++
			slog.Debug("Aggregator received EOF", "client", msg.ClientID, "eofs", w.eofsReceived[msg.ClientID], "expected", w.expectedEOFs)
			return w.eofsReceived[msg.ClientID] >= w.expectedEOFs
		})
		if shouldFinish {
			if err := w.finish(msg.ClientID); err != nil {
				slog.Error("While finishing", "err", err)
				utils.WithLock(&w.mu, func() {
					w.eofsReceived[msg.ClientID]--
				})
				time.Sleep(1 * time.Second)
				nack()
				return
			}
		}
		ack()
		return
	}
	if msg.MsgCode != w.expectedMsgCode {
		nack()
		return
	}

	result, err := w.handler.DeserializeQueryResult([]byte(msg.Body))
	if err != nil {
		slog.Error("While parsing results", "err", err)
		nack()
		return
	}
	batchBytes, err := result.ToBytes()
	if err != nil {
		slog.Error("While serializing results", "err", err)
		nack()
		return
	}
	if err := w.storage.Write(msg.ClientID, batchBytes); err != nil {
		slog.Error("While storing results", "err", err)
		nack()
		return
	}
	ack()
}

func (w *Aggregator[T]) finish(clientID uint64) error {
	alreadyDone := utils.WithLockAndReturn(&w.mu, func() bool {
		if w.clientDone[clientID] {
			return true
		}
		w.clientDone[clientID] = true
		return false
	})
	if alreadyDone {
		return nil
	}

	batchBytes, err := w.storage.Read(clientID)
	if err != nil {
		utils.WithLock(&w.mu, func() {
			delete(w.clientDone, clientID)
		})
		return err
	}
	resultBatch := w.handler.NewEmptyBatch(0)
	for _, b := range batchBytes {
		result, err := w.handler.DeserializeArticleBatch(b)
		if err != nil {
			utils.WithLock(&w.mu, func() {
				delete(w.clientDone, clientID)
			})
			return err
		}
		resultBatch.Merge(*result)
	}
	finalResult := w.handler.BuildQueryResult(resultBatch)

	if err := w.publishQueryResult(clientID, finalResult); err != nil {
		utils.WithLock(&w.mu, func() {
			delete(w.clientDone, clientID)
		})
		return err
	}

	w.sendEOF(clientID)

	utils.WithLock(&w.mu, func() {
		delete(w.clientDone, clientID)
		delete(w.eofsReceived, clientID)
	})
	return nil
}
func (w *Aggregator[T]) publishQueryResult(clientID uint64, result *qresults.QueryResult) error {
	resultBytes, err := result.ToBytes()
	if err != nil {
		return err
	}

	message := middleware.Message{
		ClientID: clientID,
		MsgCode:  messages.ResultCode,
		Body:     resultBytes,
	}
	if err := w.sendToGateway(clientID, message); err != nil {
		return err
	}
	slog.Info("[AGGREGATOR] Query resolved and published to gateway", "query", w.queryCode, "client", clientID)
	return nil
}

func (w *Aggregator[T]) sendEOF(clientID uint64) {
	eof := messages.EofMsg{}
	eofMessage, err := middleware.NewMessage(clientID, messages.EOFCode, &eof)
	if err != nil {
		slog.Error("While creating EOF message", "err", err)
		return
	}
	if err := w.sendToGateway(clientID, *eofMessage); err != nil {
		slog.Error("While sending EOF message", "err", err)
	}
}

func (w *Aggregator[T]) sendToGateway(clientID uint64, message middleware.Message) error {
	targetNode := utils.ExtractGatewayId(clientID)
	routingKey := utils.GenerateRoutingKey(w.gatewayPrefix, int(targetNode))
	if err := w.outputExchange.SendToKey(message, routingKey); err != nil {
		return err
	}
	return nil
}
