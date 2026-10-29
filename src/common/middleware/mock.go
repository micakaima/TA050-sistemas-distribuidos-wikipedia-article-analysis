package middleware

import "sync"

// MockMiddleware permite testear la lógica de los nodos sin necesidad de levantar RabbitMQ.
type MockMiddleware struct {
	mu           sync.Mutex
	SentMessages []Message
	Consumer     func(msg Message, ack func(), nack func())
	Closed       bool
}

func NewMockMiddleware() *MockMiddleware {
	return &MockMiddleware{
		SentMessages: make([]Message, 0),
	}
}

func (m *MockMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	m.mu.Lock()
	m.Consumer = callbackFunc
	m.mu.Unlock()
	return nil
}

func (m *MockMiddleware) StopConsuming() error {
	return nil
}

func (m *MockMiddleware) Send(msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SentMessages = append(m.SentMessages, msg)
	return nil
}

func (m *MockMiddleware) SendToKey(msg Message, routingKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SentMessages = append(m.SentMessages, msg)
	return nil
}

func (m *MockMiddleware) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Closed = true
	return nil
}

func (m *MockMiddleware) SimulateIncomingMessage(body string) (acked bool, nacked bool) {
	return m.SimulateIncomingMessageWithClientID(0, body)
}

func (m *MockMiddleware) SimulateIncomingMessageWithClientID(clientID uint64, body string) (acked bool, nacked bool) {
	m.mu.Lock()
	cb := m.Consumer
	m.mu.Unlock()

	if cb == nil {
		return false, false
	}

	ackCalled := false
	nackCalled := false
	cb(Message{ClientID: clientID, Body: body}, func() { ackCalled = true }, func() { nackCalled = true })
	return ackCalled, nackCalled
}
