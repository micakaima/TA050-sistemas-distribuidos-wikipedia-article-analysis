package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeMiddleware struct {
	commonMiddleware
	exchange string
	keys     []string
}

func (m *exchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
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

	return m.consume(anonymousQueue.Name, callbackFunc)
}

func (m *exchangeMiddleware) Send(msg Message) error {
	for _, key := range m.keys {
		err := m.channel.Publish(
			m.exchange,
			key,
			false,
			false,
			amqp.Publishing{
				Headers: amqp.Table{
					ClientIDHeader: int64(msg.ClientID),
				},
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})
		if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func (m *exchangeMiddleware) SendToKey(msg Message, routingKey string) error {
	err := m.channel.Publish(
		m.exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table{
				ClientIDHeader: int64(msg.ClientID),
			},
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		return ErrMessageMiddlewareMessage
	}
	return nil
}
