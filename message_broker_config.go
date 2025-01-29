package messagebroker

type MessageBrokerConfig struct {
	URL                  string
	HasExchange          bool
	Exchange             *MessageBrokerConfigExchange
	Consumer             *MessageBrokerConfigConsumer
	PrefetchCount        int
	ShouldAckInmediately bool
}

type MessageBrokerConfigExchange struct {
	Name    string
	Type    string
	Durable bool
}

type MessageBrokerConfigConsumer struct {
	Name string
}
