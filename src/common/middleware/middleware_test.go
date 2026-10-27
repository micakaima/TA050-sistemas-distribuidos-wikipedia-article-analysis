package middleware

import (
	"testing"
)

func TestMockMiddlewareSendAndConsume(t *testing.T) {
	mock := NewMockMiddleware()

	var received string
	err := mock.StartConsuming(func(msg Message, ack func(), nack func()) {
		received = msg.Body
		ack()
	})
	if err != nil {
		t.Fatalf("unexpected error starting consuming: %v", err)
	}

	acked, nacked := mock.SimulateIncomingMessage("test payload")
	if !acked {
		t.Errorf("expected message to be acked")
	}
	if nacked {
		t.Errorf("expected message not to be nacked")
	}
	if received != "test payload" {
		t.Errorf("expected payload 'test payload', got '%s'", received)
	}

	if err := mock.Send(Message{Body: "outgoing"}); err != nil {
		t.Fatalf("unexpected error on Send: %v", err)
	}
	if len(mock.SentMessages) != 1 || mock.SentMessages[0].Body != "outgoing" {
		t.Errorf("expected SentMessages to contain 'outgoing'")
	}
}

// TestNodeWithMultipleOutputQueues verifica que cada cola de salida
// sea una instancia dedicada y el envío no requiera routing keys.
func TestNodeWithMultipleOutputQueues(t *testing.T) {
	candidatesQueue := NewMockMiddleware()
	abstractsQueue := NewMockMiddleware()

	_ = candidatesQueue.Send(Message{Body: "article_url,2020,es,name"})
	_ = abstractsQueue.Send(Message{Body: "article_url,2020,abstract_text,es"})

	if len(candidatesQueue.SentMessages) != 1 {
		t.Fatalf("expected 1 message in candidatesQueue, got %d", len(candidatesQueue.SentMessages))
	}
	if candidatesQueue.SentMessages[0].Body != "article_url,2020,es,name" {
		t.Errorf("unexpected body in candidatesQueue")
	}

	if len(abstractsQueue.SentMessages) != 1 {
		t.Fatalf("expected 1 message in abstractsQueue, got %d", len(abstractsQueue.SentMessages))
	}
	if abstractsQueue.SentMessages[0].Body != "article_url,2020,abstract_text,es" {
		t.Errorf("unexpected body in abstractsQueue")
	}
}
