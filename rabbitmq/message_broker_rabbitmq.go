package rabbitmq

/*
Package rabbitmq provides a RabbitMQ implementation of the message broker interface.

This package contains functionality to interact with RabbitMQ message broker, including:
- Connection and channel management
- Publishing and consuming messages
- Error handling
- Message serialization/deserialization using JSON

Import statement includes required dependencies:
- encoding/json: For JSON marshaling/unmarshaling of messages
- errors: For error handling and wrapping
- fmt: For string formatting and error messages
- sync: For synchronization primitives
- time: For timeout and timing operations
- messagebroker: Core message broker interfaces and types
- amqp: The official RabbitMQ client library for Go

The package implements the messagebroker.MessageBroker interface defined in the
github.com/spaval/messagebroker package.
*/
import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/spaval/messagebroker"
	"github.com/streadway/amqp"
)

type MessageBrokerRabbitMQ struct {
	config             messagebroker.MessageBrokerConfig
	observer           *RabbitObserver
	loading            bool
	lock               sync.RWMutex
	notifyCloseChannel chan *amqp.Error
	Connection         *amqp.Connection
	ConsumerChannel    *amqp.Channel
	PublisherChannel   *amqp.Channel
}

func NewMessageBrokerRabbitMQ(config messagebroker.MessageBrokerConfig, observer *RabbitObserver, notifyCloseChannel chan *amqp.Error) (*MessageBrokerRabbitMQ, error) {
	broker := &MessageBrokerRabbitMQ{
		config:             config,
		observer:           observer,
		notifyCloseChannel: notifyCloseChannel,
	}

	conn, err := broker.Connect()
	if err != nil {
		return nil, err
	}

	broker.Connection = conn

	return broker, nil
}

func (b *MessageBrokerRabbitMQ) Connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(b.config.URL)
	if err != nil {
		return nil, err
	}

	b.Connection = conn
	conn.NotifyClose(b.notifyCloseChannel)

	consumerChannel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	b.ConsumerChannel = consumerChannel
	consumerChannel.NotifyClose(b.notifyCloseChannel)

	publisherChannel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	b.PublisherChannel = publisherChannel
	publisherChannel.NotifyClose(b.notifyCloseChannel)

	if b.config.PrefetchCount > 0 {
		if err := consumerChannel.Qos(b.config.PrefetchCount, 0, false); err != nil {
			return nil, err
		}
	}

	return conn, nil
}

func (b *MessageBrokerRabbitMQ) Notify() {
	b.lock.RLock()

	reconnectionLoading := b.loading

	if reconnectionLoading {
		b.lock.RUnlock()
		return
	}

	b.lock.RUnlock()

	b.lock.Lock()
	b.loading = true
	b.lock.Unlock()

	if !b.config.ShouldReconnect {
		if !b.Connection.IsClosed() {
			b.Connection.Close()
		}

		err := fmt.Errorf("error: channel or connection lost and should reconnect is %v", b.config.ShouldReconnect)
		panic(err)
	}

	retries := 1

	for {

		if b.Connection.IsClosed() {
			<-time.After(time.Duration(b.config.ReconnectDelay) * time.Second)

			_, err := b.Connect()
			if err != nil {
				if retries >= b.config.RetriesCount {
					err = fmt.Errorf("error: max retries (%d) connection to rabbit", b.config.RetriesCount)
					panic(err)
				}

				retries++

				continue
			}
		}

		b.lock.Lock()
		b.loading = false
		b.lock.Unlock()

		b.observer.NotifyAll()

		break
	}
}

func (b *MessageBrokerRabbitMQ) Publish(options messagebroker.MessageBrokerDeliveryOptions, message any) error {
	if b.Connection.IsClosed() {
		return errors.New("The connection is close")
	}

	var body []byte
	var err error

	if err = setupExchangeDeclare(b.PublisherChannel, options); err != nil {
		return err
	}

	if body, err = json.Marshal(message); err != nil {
		return err
	}

	return b.PublisherChannel.Publish(
		options.ExchangeName,
		options.RoutingKey,
		false,
		false,
		amqp.Publishing{
			DeliveryMode:  amqp.Persistent,
			ContentType:   "application/json",
			CorrelationId: options.CorrelationID,
			Body:          body,
			Timestamp:     time.Now(),
		})
}

func (b *MessageBrokerRabbitMQ) Consume(options messagebroker.MessageBrokerDeliveryOptions, success chan messagebroker.MessageBrokerPayload, fail chan error) error {
	if b.Connection.IsClosed() {
		return errors.New("The connection is close")
	}

	if err := setupExchangeDeclare(b.ConsumerChannel, options); err != nil {
		return err
	}

	if _, err := setupQueue(b.ConsumerChannel, options); err != nil {
		return err
	}

	if err := setupQueueBind(b.ConsumerChannel, options); err != nil {
		return err
	}

	messages, err := b.ConsumerChannel.Consume(
		options.QueueName,
		options.ConsumerTag,
		options.NoAck,
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
