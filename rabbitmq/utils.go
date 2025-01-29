package rabbit

import (
	"log"

	"github.com/spaval/messagebroker"
	"github.com/streadway/amqp"
)

func setupQueue(channel *amqp.Channel, queueName string) error {
	_, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("error declaring the queue: %s. Error: %s\n", queueName, err.Error())
		return err
	}

	return nil
}

func setupExchangeDeclare(channel *amqp.Channel, config messagebroker.MessageBrokerConfigExchange) error {
	err := channel.ExchangeDeclare(
		config.Name,
		config.Type,
		config.Durable,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("error declaring the exchange: %s. Error: %s\n", config.Name, err.Error())
		return err
	}

	return nil
}
