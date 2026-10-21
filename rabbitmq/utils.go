package rabbitmq

import (
	"github.com/spaval/messagebroker"
	"github.com/streadway/amqp"
)

func setupQueue(channel *amqp.Channel, config messagebroker.MessageBrokerDeliveryOptions) (amqp.Queue, error) {
	q, err := channel.QueueDeclare(
		config.QueueName,
		config.MessageDurable,
		config.DeleteWhenUnused,
		config.Exclusive,
		config.NoWait,
		toAMQPTable(config.QueueArguments),
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

// toAMQPTable builds the arguments table of a queue, leaving out every argument
// that was not configured. Sending an empty value instead would make the broker
// apply it (an empty dead letter exchange, a TTL of zero) and would also break
// redeclaring queues created before these arguments existed.
//
// With nothing configured it returns nil, the same value the library sent
// before these arguments existed, so apps that do not set them declare exactly
// the same queues.
func toAMQPTable(args messagebroker.QueueArguments) amqp.Table {
	if args == (messagebroker.QueueArguments{}) {
		return nil
	}

	table := amqp.Table{}

	if args.MessageTTL > 0 {
		table["x-message-ttl"] = args.MessageTTL
	}
	if args.DeadLetterExchange != "" {
		table["x-dead-letter-exchange"] = args.DeadLetterExchange
	}
	if args.DeadLetterRoutingKey != "" {
		table["x-dead-letter-routing-key"] = args.DeadLetterRoutingKey
	}
	if args.XQueueType != "" {
		table["x-queue-type"] = args.XQueueType
	}
	if args.DeliveryLimit > 0 {
		table["x-delivery-limit"] = args.DeliveryLimit
	}

	return table
}
