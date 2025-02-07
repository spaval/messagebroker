package messagebroker

type MessageBroker interface {
	Notify()
	Publish(options MessageBrokerDeliveryOptions, message any) error
	Consume(options MessageBrokerDeliveryOptions, success chan MessageBrokerPayload, fail chan error) error
}
