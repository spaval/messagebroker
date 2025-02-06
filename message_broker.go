package messagebroker

type MessageBroker interface {
	Notify()
	Publish(queueName string, correlationID string, message any) error
	Consume(queueName string, data chan MessageBrokerPayload, fail chan error) error
}
