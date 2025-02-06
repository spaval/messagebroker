package messagebroker

type MessageBrokerConfig struct {
	URL                  string
	ShouldAckInmediately bool
	ShouldReconnect      bool
	ReconnectDelay       int
	RetriesCount         int
	PrefetchCount        int
}

type MessageBrokerDeliveryOptions struct {
	HasExchange    bool
	ExchangeName   string
	ExchangeType   string
	MessageDurable bool
	RoutingKey     string
	QueueName      string
	ConsumerTag    string
	CorrelationID  string
	NoAck          bool
}
