package middleware

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const prefetchCount = 1
const prefetchSize = 0

type commonMiddleware struct {
	connection  *amqp.Connection
	channel     *amqp.Channel
	consumerTag string
}

func (m *commonMiddleware) consume(queueName string, callbackFunc func(msg Message, ack func(), nack func())) error {
	if err := m.channel.Qos(prefetchCount, prefetchSize, false); err != nil {
		return ErrMessageMiddlewareMessage
	}

	m.consumerTag = fmt.Sprintf("consumer_%s_%d", queueName, time.Now().UnixNano())

	messages, err := m.channel.Consume(
		queueName,
		m.consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	for delivery := range messages {
		msg := Message{Body: string(delivery.Body)}
		deliveryTag := delivery.DeliveryTag
		ack := func() { _ = m.channel.Ack(deliveryTag, false) }
		nack := func() { _ = m.channel.Nack(deliveryTag, false, true) }
		callbackFunc(msg, ack, nack)
	}

	return nil
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
