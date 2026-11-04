package columnfiltermanager

import (
	"fmt"
	"log/slog"
	"math/rand/v2"

	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/middleware"
	"TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"
	errors "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/utils"

	"os"
	"os/signal"
	"sync"
	"syscall"
)

type ColumnFilterManagerConfig struct {
	Id                 int
	MomHost            string
	MomPort            int
	InputQueue         string
	ColumnFilterAmount int
	ColumnFilterPrefix string
}

type ColumnFilterManager struct {
	inputQueue         middleware.Middleware
	outputExchange     middleware.ExchangeMiddleware
	columnFilterAmount int
	columnFilterPrefix string
	workerKeys         []string
	nextTargetNodeIdx  int
}

func NewColumnFilterManager(config ColumnFilterManagerConfig) (*ColumnFilterManager, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.ColumnFilterPrefix, []string{}, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	workerKeys := make([]string, 0, config.ColumnFilterAmount)
	for i := range config.ColumnFilterAmount {
		workerKeys = append(workerKeys, utils.GenerateRoutingKey(config.ColumnFilterPrefix, i))
	}
	if len(workerKeys) == 0 {
		return nil, fmt.Errorf(errors.NotEnoughWorkers)
	}
	firstTargetNodeIdx := rand.IntN(len(workerKeys))

	return &ColumnFilterManager{
		inputQueue:         inputQueue,
		outputExchange:     outputExchange,
		columnFilterAmount: config.ColumnFilterAmount,
		columnFilterPrefix: config.ColumnFilterPrefix,
		workerKeys:         workerKeys,
		nextTargetNodeIdx:  firstTargetNodeIdx,
	}, nil
}

func (m *ColumnFilterManager) Run() {
	done := make(chan struct{})
	m.handleSignals(done)
	defer close(done)
	var wg sync.WaitGroup

	utils.SpawnWithWG(&wg, func() {
		m.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			m.handleMessage(msg, ack, nack)
		})
	})

	wg.Wait()
	m.shutdown()
}

func (m *ColumnFilterManager) handleSignals(done chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-signals:
			slog.Info("Graceful shutdown started...")
			m.inputQueue.StopConsuming()
		case <-done:
			return
		}
	}()
}

func (m *ColumnFilterManager) shutdown() {
	if err := m.inputQueue.Close(); err != nil {
		slog.Error("While closing input queue", "err", err)
	}
	if err := m.outputExchange.Close(); err != nil {
		slog.Error("While closing output queue", "err", err)
	}
}

func (m *ColumnFilterManager) handleMessage(msg middleware.Message, ack, nack func()) {
	switch msg.MsgCode {
	case messages.ArticlesBatchCode:
		if err := m.handleDataMsg(msg); err != nil {
			slog.Error("While handling Articles Batch message")
			nack()
			return
		}
	case messages.EOFCode:
		if err := m.handleEOF(msg); err != nil {
			slog.Error("While handling EOF message")
			nack()
			return
		}
	default:
		slog.Debug("Received unexpected inner message code")
		nack()
		return
	}
	ack()
}

func (m *ColumnFilterManager) handleDataMsg(msg middleware.Message) error {
	targetNodeKey := m.getNextTargetNode()
	if err := m.outputExchange.SendToKey(msg, targetNodeKey); err != nil {
		slog.Debug("While sending Data msg to worker", "err", err)
		return err
	}
	return nil
}

func (m *ColumnFilterManager) handleEOF(msg middleware.Message) error {
	eof := messages.EofMsg{}
	eofMsg, err := middleware.NewMessage(msg.ClientID, messages.EOFCode, &eof)
	if err != nil {
		return err
	}
	for _, key := range m.workerKeys {
		if err := m.outputExchange.SendToKey(*eofMsg, key); err != nil {
			slog.Debug("While sending EOF msg to worker", "err", err)
		}
	}
	return nil
}

func (m *ColumnFilterManager) getNextTargetNode() string {
	nextTargetNode := m.workerKeys[m.nextTargetNodeIdx]
	m.nextTargetNodeIdx = (m.nextTargetNodeIdx + 1) % len(m.workerKeys)
	return nextTargetNode
}
