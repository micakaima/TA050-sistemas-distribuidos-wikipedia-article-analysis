package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type queueMiddleware struct {
	commonMiddleware
	queueName string
}

func (m *queueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return m.consume(m.queueName, callbackFunc)
}

func (m *queueMiddleware) Send(msg Message) error {
	err := m.channel.Publish(
		"",          // default exchange
		m.queueName, // routing key apunta directo a la cola de esta instancia
		false,       // mandatory
		false,       // immediate
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
