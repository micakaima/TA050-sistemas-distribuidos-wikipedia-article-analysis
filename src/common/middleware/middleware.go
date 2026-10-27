package middleware

import "errors"

var (
	ErrMessageMiddlewareMessage      = errors.New("message middleware: message error")
	ErrMessageMiddlewareDisconnected = errors.New("message middleware: disconnected")
	ErrMessageMiddlewareClose        = errors.New("message middleware: close error")
)

type Message struct {
	Body string
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
