package middleware

import (
	"errors"

	messages "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol"
	inner "TA050-sistemas-distribuidos-wikipedia-article-analysis/src/common/message_protocol/inner/messages"
)

var (
	ErrMessageMiddlewareMessage      = errors.New("message middleware: message error")
	ErrMessageMiddlewareDisconnected = errors.New("message middleware: disconnected")
	ErrMessageMiddlewareClose        = errors.New("message middleware: close error")
)

const ClientIDHeader = "client_id"

type Message struct {
	ClientID uint64
	MsgCode  inner.InnerMsgCode
	Body     string
}

func NewMessage(clientID uint64, mcode inner.InnerMsgCode, msg messages.SerializableMessage) (*Message, error) {
	msgBytes, err := msg.ToBytes()
	if err != nil {
		return nil, err
	}
	return &Message{ClientID: clientID, MsgCode: mcode, Body: string(msgBytes)}, nil
}

type Event struct {
	Source  int
	Message Message
	Ack     func()
	Nack    func()
}

type ConnSettings struct {
	Hostname string
	Port     int
}

type Middleware interface {
	// StartConsuming comienza a escuchar a la cola o exchange e invoca callbackFunc tras cada mensaje.
	StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error

	// StopConsuming detiene la escucha de mensajes si estaba activa.
	StopConsuming() error

	// Send envía un mensaje al destino preconfigurado en esta instancia.
	Send(msg Message) error

	// Close libera y cierra los canales y la conexión.
	Close() error
}

type ExchangeMiddleware interface {
	Middleware

	// SendToKey envía un mensaje al exchange utilizando una routing key específica.
	SendToKey(msg Message, routingKey string) error
}
