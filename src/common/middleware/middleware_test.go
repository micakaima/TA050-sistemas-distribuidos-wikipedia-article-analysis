package middleware

import (
	"testing"
)

func TestMockMiddlewareSendAndConsume(t *testing.T) {
	mock := NewMockMiddleware()

	var received []byte
	err := mock.StartConsuming(func(msg Message, ack func(), nack func()) {
		received = msg.Body
		ack()
	})
	if err != nil {
		t.Fatalf("unexpected error starting consuming: %v", err)
	}

	acked, nacked := mock.SimulateIncomingMessage([]byte("test payload"))
	if !acked {
		t.Errorf("expected message to be acked")
	}
	if nacked {
		t.Errorf("expected message not to be nacked")
	}
	if string(received) != "test payload" {
		t.Errorf("expected payload 'test payload', got '%s'", received)
	}

	if err := mock.Send(Message{Body: []byte("outgoing")}); err != nil {
		t.Fatalf("unexpected error on Send: %v", err)
	}
	if len(mock.SentMessages) != 1 || string(mock.SentMessages[0].Body) != "outgoing" {
		t.Errorf("expected SentMessages to contain 'outgoing'")
	}
}

// TestNodeWithMultipleOutputQueues verifica que cada cola de salida
// sea una instancia dedicada y el envío no requiera routing keys.
func TestNodeWithMultipleOutputQueues(t *testing.T) {
	candidatesQueue := NewMockMiddleware()
	abstractsQueue := NewMockMiddleware()

	_ = candidatesQueue.Send(Message{Body: []byte("article_url,2020,es,name")})
	_ = abstractsQueue.Send(Message{Body: []byte("article_url,2020,abstract_text,es")})

	if len(candidatesQueue.SentMessages) != 1 {
		t.Fatalf("expected 1 message in candidatesQueue, got %d", len(candidatesQueue.SentMessages))
	}
	if string(candidatesQueue.SentMessages[0].Body) != "article_url,2020,es,name" {
		t.Errorf("unexpected body in candidatesQueue")
	}

	if len(abstractsQueue.SentMessages) != 1 {
		t.Fatalf("expected 1 message in abstractsQueue, got %d", len(abstractsQueue.SentMessages))
	}
	if string(abstractsQueue.SentMessages[0].Body) != "article_url,2020,abstract_text,es" {
		t.Errorf("unexpected body in abstractsQueue")
	}
}

func TestMockMiddlewareSendToKey(t *testing.T) {
	var mock ExchangeMiddleware = NewMockMiddleware()

	err := mock.SendToKey(Message{Body: []byte("routed_message")}, "my-key")
	if err != nil {
		t.Fatalf("unexpected error on SendToKey: %v", err)
	}

	mockConcrete := mock.(*MockMiddleware)
	if len(mockConcrete.SentMessages) != 1 || string(mockConcrete.SentMessages[0].Body) != "routed_message" {
		t.Errorf("expected SentMessages to contain 'routed_message'")
	}
}

func TestMockMiddlewareClientIDPropagation(t *testing.T) {
	mock := NewMockMiddleware()

	var received Message
	_ = mock.StartConsuming(func(msg Message, ack func(), nack func()) {
		received = msg
		ack()
	})

	expectedClientID := uint64(9876543210)
	acked, _ := mock.SimulateIncomingMessageWithClientID(expectedClientID, []byte("client payload"))
	if !acked {
		t.Errorf("expected message to be acked")
	}
	if received.ClientID != expectedClientID {
		t.Errorf("expected ClientID %d, got %d", expectedClientID, received.ClientID)
	}
	if string(received.Body) != "client payload" {
		t.Errorf("expected body 'client payload', got '%s'", received.Body)
	}
}
