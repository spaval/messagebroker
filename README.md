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
		PrefetchCount: 10,
		URL:           "amqp://guest:guest@localhost:5672/",
		Consumer: messagebroker.MessageBrokerConfigConsumer{
			AutoAck: false,
		}
	}

	conn, err := rabbitmq.NewMessageBrokerRabbitMQ(config)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ or open a channel: %s", err.Error())
	}

	e := echo.New()

	queueName := "task_q"

	e.POST("/publish", func(c echo.Context) error {
		var body map[string]any

		if err := c.Bind(&body); err != nil {
			return c.JSON(http.StatusBadRequest, err.Error())
		}

		body["time"] = time.Now().UTC()

		if err := conn.Publish(queueName, body); err != nil {
			return c.JSON(http.StatusConflict, fmt.Sprintf("error publishing. Error: %s", err.Error()))
		}

		return c.JSON(http.StatusOK, true)
	})

	go func(key string) {
		success := make(chan messagebroker.MessageBrokerPayload, 1)
		fail := make(chan error, 1)

		if err := conn.Consumer(key, success, fail); err != nil {
			log.Printf("error consuming the queue: %s. Error: %s\n", key, err.Error())
		}

		for {
			select {
			case d := <-success:
				log.Printf("Message Incoming.... %v", string(d.Body))
				d.Ack(false)
			
			case f := <-fail:
			 	log.Printf("Error consuming the queue: %s. Error: %s\n", key, f.Error())
				//conn.Publish("test", "Hola Mundo")
			}

			// business logic...
		}
	}(queueName)

	e.Start(":3000")
}

```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
