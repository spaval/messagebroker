# Message Broker

A simple message broker implementation wrapper in Go.

## Features

- Asynchronous message publishing and subscription
- Topics support

## Installation

```bash
go get github.com/spaval/messagebroker
```

## Usage

### Basic Example

```go
package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/spaval/messagebroker"
	"github.com/spaval/messagebroker/rabbitmq"
)

func main() {
	config := messagebroker.MessageBrokerConfig{
		PrefetchCount:        10,
		URL:                  "amqp://xxxx:xxxx@localhost:5672/",
		ShouldAckInmediately: true,
		ShouldReconnect:      true,
		ReconnectDelay:       3,
		RetriesCount:         5,
	}

	closing := make(chan error)
	conn, err := rabbitmq.NewMessageBrokerRabbitMQ(config, nil, closing)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ or open a channel: %s", err.Error())
	}

	defer conn.Connection.Close()
	defer conn.ConsumerChannel.Close()
	defer conn.PublisherChannel.Close()

	go func(c *rabbitmq.MessageBrokerRabbitMQ) {
		for {
			select {
			case err := <-closing:
				log.Printf("[MESSAGE-BROKER-ERROR]: %v", err)
				c.Notify()
			}
		}

	}(conn)

	e := echo.New()

	options := messagebroker.MessageBrokerDeliveryOptions{
		QueueName: "task_q",
	}

	e.POST("/publish", func(c echo.Context) error {
		var body map[string]any

		if err := c.Bind(&body); err != nil {
			return c.JSON(http.StatusBadRequest, err.Error())
		}

		body["time"] = time.Now().UTC()

		if err := conn.Publish(options, body); err != nil {
			return c.JSON(http.StatusConflict, fmt.Sprintf("error publishing. Error: %s", err.Error()))
		}

		return c.JSON(http.StatusOK, true)
	})

	go func(opts messagebroker.MessageBrokerDeliveryOptions) {
		success := make(chan messagebroker.MessageBrokerPayload, 1)
		fail := make(chan error, 1)

		if err := conn.Consume(options, success, fail); err != nil {
			log.Printf("error consuming the queue: %s. Error: %s\n", options.QueueName, err.Error())
		}

		for {
			select {
			case data := <-success:
				log.Printf("Message Incoming.... %v", string(data.Body))
				data.Ack(false)
			case <-fail:
				log.Printf("Error.... %v", <-fail)
			}
		}
	}(options)

	e.Start(":3000")
}

```

### Queue Arguments

`QueueArguments` is optional. Leave it out and the queue is declared exactly as in previous versions.

```go
options := messagebroker.MessageBrokerDeliveryOptions{
	QueueName:      "task_q",
	ExchangeName:   "tasks",
	ExchangeType:   "topic",
	RoutingKey:     "task.created",
	MessageDurable: true, // required by quorum and stream queues
	HasExchange:    true,
	QueueArguments: messagebroker.QueueArguments{
		XQueueType:           "quorum",      // "classic", "quorum" or "stream"
		MessageTTL:           60000,         // milliseconds
		DeadLetterExchange:   "tasks_dlx",   // receives rejected or expired messages
		DeadLetterRoutingKey: "task.failed", // optional, keeps the original key when empty
	},
}

if err := conn.Consume(options, success, fail); err != nil {
	log.Printf("error consuming the queue: %s. Error: %s\n", options.QueueName, err.Error())
}
```

Keep in mind:

- When `HasExchange` is true, `Publish` only declares the exchange, so the arguments take effect when `Consume` declares the queue.
- `MessageDurable` applies to both the exchange and the queue.
- RabbitMQ rejects declaring an existing queue with different arguments (`PRECONDITION_FAILED`). To change the type of an existing queue, delete it and let it be declared again.

### Stopping a subscription

`Consume` subscribes and throws the handle away. `Subscribe` hands it back, so
the subscription can be stopped later. It lives on the `Subscriber` interface,
which is kept apart from `MessageBroker` so that nothing implementing the latter
has to change:

```go
type IdentityReceiver struct {
	bus      messagebroker.MessageBroker
	previous messagebroker.Subscription
}

func (r *IdentityReceiver) Receive() error {
	options := messagebroker.MessageBrokerDeliveryOptions{
		QueueName:      "identity.validation.queue",
		ExchangeName:   "identity.validation.exchange",
		ExchangeType:   "topic",
		RoutingKey:     "identity.validation.routing.validate",
		MessageDurable: true,
		HasExchange:    true,
		QueueArguments: messagebroker.QueueArguments{
			XQueueType: "quorum",
		},
	}

	success := make(chan messagebroker.MessageBrokerPayload, 1)
	fail := make(chan error, 1)

	subscriber, ok := r.bus.(messagebroker.Subscriber)
	if !ok {
		return r.bus.Consume(options, success, fail)
	}

	// Replace the previous subscription instead of adding to it. A consumer
	// that resubscribes on every reconnection without this leaves the one
	// before running, with its own goroutine and its own consumer at the
	// broker, and the queue ends up with one more on every reconnection.
	if r.previous != nil {
		if err := r.previous.Stop(); err != nil {
			log.Printf("stopping the previous subscription: %v", err)
		}
	}

	subscription, err := subscriber.Subscribe(options, success, fail)
	if err != nil {
		return err
	}

	r.previous = subscription

	go r.process(success, fail)

	return nil
}

func (r *IdentityReceiver) process(success chan messagebroker.MessageBrokerPayload, fail chan error) {
	for {
		select {
		case msg := <-success:
			if err := r.handle(msg.Body); err != nil {
				// Hand it back only when another attempt can succeed, and bound
				// the attempts yourself: a requeued message is redelivered
				// without end, whatever delivery limit the queue carries.
				if !msg.Redelivered {
					msg.Nack(false, true)
					continue
				}

				// Out of attempts: drop it, so the queue's dead letter
				// configuration takes over if it has one.
				msg.Nack(false, false)

				continue
			}

			if err := msg.Ack(false); err != nil {
				log.Printf("acknowledging: %v", err)
			}

		// The broker closed this subscription's channel. Nothing else of the
		// process is affected, but this subscription is done until it is set
		// up again.
		case err := <-fail:
			log.Printf("the subscription stopped: %v", err)

			return
		}
	}
}
```

## Testing

Unit tests need nothing and run on every change:

```bash
go test ./... -race
```

The integration tests run against a real broker and are behind a build tag, so
they never run by accident:

```bash
podman run -d --name mb-rabbit -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=test -e RABBITMQ_DEFAULT_PASS=test rabbitmq:4-management

go test ./rabbitmq/ -tags integration -count=1
```

They cover publishing and consuming through an exchange and straight to a queue,
the three acknowledgements, prefetch, the queue arguments, and what the library
does when the broker drops the connection or closes a channel. Assertions about
what the broker actually did go through its management API.

To run them against another broker version — the one production is on, for
instance — point them elsewhere:

```bash
podman run -d --name mb-rabbit43 -p 5673:5672 -p 15673:15672 \
  -e RABBITMQ_DEFAULT_USER=test -e RABBITMQ_DEFAULT_PASS=test rabbitmq:4.3.6-management

MB_AMQP_URL=amqp://test:test@localhost:5673/ \
MB_MANAGEMENT_URL=http://localhost:15673/api \
  go test ./rabbitmq/ -tags integration -count=1
```

A publish costs one frame, not a declaration round trip: each exchange and queue
is declared once per connection and remembered until the connection is rebuilt.
`BenchmarkPublish` reports the difference, around 880us per publish before
against around 72us after. The one consequence to know about is that a queue
deleted from under the application is no longer brought back by the next
publish; it is declared again on the next reconnection.

Every subscription consumes on a channel of its own, so a queue whose
declaration the broker refuses costs only that subscription instead of every
consumer in the process. A subscription whose channel the broker closes is told
through the `fail` channel it handed to `Consume`.

Two of them say as much about the broker as about this library, and are worth
reading before designing around them:

- `TestRequeueingIsNotBoundedByTheDeliveryLimit` — a message handed back with
  `Nack(requeue)` is redelivered without end. Neither `DeliveryLimit` nor a
  broker policy stops it, because only an involuntary return counts against the
  limit. Requeueing is not a way to say "this one is not mine".
- `TestTransientQueuesAreRefusedOutright` — current brokers refuse a non
  durable, non exclusive queue by default, so `MessageDurable` is not optional
  any more. It skips on a broker that still permits them.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
