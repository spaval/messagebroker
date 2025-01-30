package rabbitmq

import (
	"encoding/json"
	"time"

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

	var exchangeName string
	if b.config.Exchange != nil {
		exchangeName = b.config.Exchange.Name
	}

	return b.Channel.Publish(
		exchangeName,
		queueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
			Timestamp:    time.Now(),
		})
}

func (b *MessageBrokerRabbitMQ) Consume(queueName string, success chan messagebroker.MessageBrokerPayload, fail chan error) error {
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
		b.config.Consumer.AutoAck,
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
			payload := messagebroker.MessageBrokerPayload{
				Body:          msg.Body,
				RoutingKey:    msg.RoutingKey,
				CorrelationID: msg.CorrelationId,
				ContentType:   msg.ContentType,
				Exchange:      msg.Exchange,
				Ack:           msg.Ack,
			}

			success <- payload
		}
	}(b.config)

	return err
}
