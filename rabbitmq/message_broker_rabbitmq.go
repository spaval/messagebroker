package rabbitmq

import (
	"encoding/json"
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
	notifyCloseChannel chan error
	Connection         *amqp.Connection
	ConsumerChannel    *amqp.Channel
	PublisherChannel   *amqp.Channel
}

func NewMessageBrokerRabbitMQ(config messagebroker.MessageBrokerConfig, observer *RabbitObserver, notifyCloseChannel chan error) (*MessageBrokerRabbitMQ, error) {
	broker := &MessageBrokerRabbitMQ{
		config:             config,
		observer:           observer,
		notifyCloseChannel: notifyCloseChannel,
	}

	conn, err := broker.connect()
	if err != nil {
		return nil, err
	}

	broker.Connection = conn

	return broker, nil
}

func (b *MessageBrokerRabbitMQ) Notify() {
	b.lock.RLock()
	reconnectionLoading := b.loading
	b.lock.RUnlock()

	if reconnectionLoading {
		return
	}

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
		if b.Connection.IsClosed() || b.ConsumerChannel == nil || isChannelClosed(b.ConsumerChannel) || b.PublisherChannel == nil || isChannelClosed(b.PublisherChannel) {
			<-time.After(time.Duration(b.config.ReconnectDelay) * time.Second)

			_, err := b.connect()
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
		if _, err := b.connect(); err != nil {
			return err
		}
	}

	var body []byte
	var err error

	queueName := options.QueueName

	if options.HasExchange {
		queueName = options.RoutingKey

		if err = setupExchangeDeclare(b.PublisherChannel, options); err != nil {
			return err
		}
	} else {
		if _, err := setupQueue(b.PublisherChannel, options); err != nil {
			return err
		}
	}

	if v, ok := message.([]byte); !ok {
		if body, err = json.Marshal(message); err != nil {
			return err
		}
	} else {
		body = v
	}

	return b.PublisherChannel.Publish(
		options.ExchangeName,
		queueName,
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
		if _, err := b.connect(); err != nil {
			return err
		}
	}

	if _, err := setupQueue(b.ConsumerChannel, options); err != nil {
		return err
	}

	if options.HasExchange {
		if err := setupExchangeDeclare(b.ConsumerChannel, options); err != nil {
			return err
		}

		if err := setupQueueBind(b.ConsumerChannel, options); err != nil {
			return err
		}
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

func (b *MessageBrokerRabbitMQ) connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(b.config.URL)
	if err != nil {
		return nil, err
	}

	connectionClosingChannel := make(chan *amqp.Error)
	publisherClosingChannel := make(chan *amqp.Error)
	consumerClosingChannel := make(chan *amqp.Error)

	b.Connection = conn
	conn.NotifyClose(connectionClosingChannel)

	consumerChannel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	b.ConsumerChannel = consumerChannel
	consumerChannel.NotifyClose(consumerClosingChannel)

	publisherChannel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	b.PublisherChannel = publisherChannel
	publisherChannel.NotifyClose(publisherClosingChannel)

	if b.config.PrefetchCount > 0 {
		if err := consumerChannel.Qos(b.config.PrefetchCount, 0, false); err != nil {
			return nil, err
		}
	}

	go func() {
		select {
		case err := <-connectionClosingChannel:
			b.notifyCloseChannel <- err
		case err := <-publisherClosingChannel:
			b.notifyCloseChannel <- err
		case err := <-consumerClosingChannel:
			b.notifyCloseChannel <- err
		}
	}()

	return conn, nil
}

func isChannelClosed(ch *amqp.Channel) bool {
	errChan := make(chan *amqp.Error, 1)
	ch.NotifyClose(errChan)

	select {
	case err := <-errChan:
		if err != nil {
			return true
		}
	default:
	}
	return false
}
