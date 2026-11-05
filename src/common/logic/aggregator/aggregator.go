package aggregator

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	generic "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/generic"
	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	qresults "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/querys_results"
	utils "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
)

type Config[T generic.SerializableMessage] struct {
	MomHost         string
	MomPort         int
	InputQueue      string
	OutputQueue     string
	StorageDir      string
	StorageName     string
	QueryCode       qresults.QueryResultCode
	ExpectedMsgCode messages.InnerMsgCode
	ExpectedEOFs    int
	Handler         qresults.QueryResultHandler[T]
}

type Aggregator[T generic.SerializableMessage] struct {
	inputQueue      middleware.Middleware
	outputQueue     middleware.Middleware
	storage         *Storage
	queryCode       qresults.QueryResultCode
	expectedMsgCode messages.InnerMsgCode
	expectedEOFs    int
	handler         qresults.QueryResultHandler[T]

	mu           sync.Mutex
	eofsReceived map[uint64]int
	clientDone   map[uint64]bool
}

func New[T generic.SerializableMessage](config Config[T]) (*Aggregator[T], error) {
	settings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, settings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, settings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	expectedEOFs := config.ExpectedEOFs
	if expectedEOFs <= 0 {
		expectedEOFs = 1
	}

	return &Aggregator[T]{
		inputQueue:      inputQueue,
		outputQueue:     outputQueue,
		storage:         NewStorage(config.StorageName, config.StorageDir),
		queryCode:       config.QueryCode,
		expectedMsgCode: config.ExpectedMsgCode,
		expectedEOFs:    expectedEOFs,
		handler:         config.Handler,
		eofsReceived:    make(map[uint64]int),
		clientDone:      make(map[uint64]bool),
	}, nil
}

func (w *Aggregator[T]) Run() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	var waitGroup sync.WaitGroup
	utils.SpawnWithWG(&waitGroup, func() {
		if err := w.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			w.handleMessage(msg, ack, nack)
		}); err != nil {
			slog.Error("Consumer stopped", "err", err)
		}
	})

	<-signals
	w.inputQueue.StopConsuming()
	waitGroup.Wait()
	w.shutdown()
}

func (w *Aggregator[T]) shutdown() {
	if err := w.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := w.outputQueue.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
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
		Body:     string(resultBytes),
	}
	if err := w.outputQueue.Send(message); err != nil {
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
	if err := w.outputQueue.Send(*eofMessage); err != nil {
		slog.Error("While sending EOF message", "err", err)
		return
	}
}
