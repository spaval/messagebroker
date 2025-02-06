package rabbitmq

import (
	"github.com/spaval/messagebroker"
	"github.com/streadway/amqp"
)

func setupQueue(channel *amqp.Channel, config messagebroker.MessageBrokerDeliveryOptions) (amqp.Queue, error) {
	q, err := channel.QueueDeclare(
		config.QueueName,
		config.MessageDurable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return amqp.Queue{}, err
	}

	return q, nil
}

func setupExchangeDeclare(channel *amqp.Channel, config messagebroker.MessageBrokerDeliveryOptions) error {
	err := channel.ExchangeDeclare(
		config.ExchangeName,
		config.ExchangeType,
		config.MessageDurable,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	return nil
}

func setupQueueBind(channel *amqp.Channel, config messagebroker.MessageBrokerDeliveryOptions) error {
	err := channel.QueueBind(
		config.QueueName,
		config.RoutingKey,
		config.ExchangeName,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	return nil
}
