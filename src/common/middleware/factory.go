package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func connect(settings ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)
	connection, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, ErrMessageMiddlewareDisconnected
	}

	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, nil, ErrMessageMiddlewareDisconnected
	}

	return connection, channel, nil
}

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	connection, channel, err := connect(connectionSettings)
	if err != nil {
		return nil, err
	}

	_, err = channel.QueueDeclare(
		queueName,
		false, // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	)
	if err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return nil, ErrMessageMiddlewareDisconnected
	}

	return &queueMiddleware{
		commonMiddleware: commonMiddleware{connection: connection, channel: channel},
		queueName:        queueName,
	}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	connection, channel, err := connect(connectionSettings)
	if err != nil {
		return nil, err
	}

	err = channel.ExchangeDeclare(
		exchange,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return nil, ErrMessageMiddlewareDisconnected
	}

	return &exchangeMiddleware{
		commonMiddleware: commonMiddleware{connection: connection, channel: channel},
		exchange:         exchange,
		keys:             keys,
	}, nil
}
