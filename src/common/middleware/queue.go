package middleware

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const prefetchCount = 1
const prefetchSize = 0

type queueMiddleware struct {
	commonMiddleware
	queueName string
}

func (m *queueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if err := m.channel.Qos(prefetchCount, prefetchSize, false); err != nil {
		return ErrMessageMiddlewareMessage
	}

	m.consumerTag = fmt.Sprintf("consumer_%s_%d", m.queueName, time.Now().UnixNano())

	messages, err := m.channel.Consume(
		m.queueName,
		m.consumerTag,
		false, // auto-ack desactivado
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

func (m *queueMiddleware) Send(msg Message) error {
	err := m.channel.Publish(
		"",          // default exchange
		m.queueName, // routing key apunta directo a la cola de esta instancia
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		return ErrMessageMiddlewareMessage
	}
	return nil
}
