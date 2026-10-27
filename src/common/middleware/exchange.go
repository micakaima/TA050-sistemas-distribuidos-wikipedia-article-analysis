package middleware

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeMiddleware struct {
	commonMiddleware
	exchange string
	keys     []string
}

func (m *exchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if err := m.channel.Qos(1, 0, false); err != nil {
		return ErrMessageMiddlewareMessage
	}

	anonymousQueue, err := m.channel.QueueDeclare(
		"",    // nombre vacío, genera uno aleatorio exclusivo
		false, // durable
		true,  // autoDelete
		true,  // exclusive
		false, // noWait
		nil,
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	for _, key := range m.keys {
		err = m.channel.QueueBind(
			anonymousQueue.Name,
			key,
			m.exchange,
			false,
			nil,
		)
		if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}

	m.consumerTag = fmt.Sprintf("consumer_%s_%d", anonymousQueue.Name, time.Now().UnixNano())

	messages, err := m.channel.Consume(
		anonymousQueue.Name,
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

func (m *exchangeMiddleware) Send(msg Message) error {
	for _, key := range m.keys {
		err := m.channel.Publish(
			m.exchange,
			key,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})
		if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}
	return nil
}
