package messagebroker

type MessageBroker interface {
	Publish(queueName string, message any) error
	Consume(queueName string, data chan MessageBrokerPayload, fail chan error) error
}
