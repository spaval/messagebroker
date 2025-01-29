package rabbit

import (
	"encoding/json"

	"github.com/spaval/messagebroker"
	"github.com/streadway/amqp"
)

type MessageBrokerRabbitMQ struct {
	config     messagebroker.MessageBrokerConfig
	Connection *amqp.Connection
	Channel    *amqp.Channel
}

func NewMessageBrokerRabbitMQ(config messagebroker.MessageBrokerConfig) (*MessageBrokerRabbitMQ, error) {
	conn, err := amqp.Dial(config.URL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if config.Exchange != nil {
		setupExchangeDeclare(ch, *config.Exchange)
	}

	if config.PrefetchCount > 0 {
		if err := ch.Qos(config.PrefetchCount, 0, false); err != nil {
			return nil, err
		}
	}

	broker := &MessageBrokerRabbitMQ{
		config:     config,
		Connection: conn,
		Channel:    ch,
	}

	return broker, nil
}

func (b *MessageBrokerRabbitMQ) Publish(queueName string, message any) error {
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return b.Channel.Publish(
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		})
}

func (b *MessageBrokerRabbitMQ) Consumer(queueName string, success chan any, fail chan error) error {
	if err := setupQueue(b.Channel, queueName); err != nil {
		return err
	}

	var consumerName string
	if b.config.Consumer != nil {
		consumerName = b.config.Consumer.Name
	}

	messages, err := b.Channel.Consume(
		queueName,
		consumerName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func(c messagebroker.MessageBrokerConfig) {
		for msg := range messages {
			if c.ShouldAckInmediately {
				if err := msg.Ack(false); err != nil {
					fail <- err
					continue
				}
			}

			success <- msg.Body
		}
	}(b.config)

	return err
}
