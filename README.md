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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
