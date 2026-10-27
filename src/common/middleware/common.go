package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type commonMiddleware struct {
	connection  *amqp.Connection
	channel     *amqp.Channel
	consumerTag string
}

func (m *commonMiddleware) StopConsuming() error {
	if m.consumerTag != "" && m.channel != nil {
		if err := m.channel.Cancel(m.consumerTag, false); err != nil {
			return ErrMessageMiddlewareDisconnected
		}
	}
	return nil
}

func (m *commonMiddleware) Close() error {
	if m.channel != nil {
		_ = m.channel.Close()
	}

	if m.connection != nil {
		if err := m.connection.Close(); err != nil {
			return ErrMessageMiddlewareClose
		}
	}

	return nil
}

func (m *commonMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return nil
}

func (m *commonMiddleware) Send(msg Message) error {
	return nil
}
