package messagebroker

type MessageBrokerPayload struct {
	Body          []byte
	CorrelationID string
	ContentType   string
	Exchange      string
	DeliveryMode  uint8
	RoutingKey    string
	Ack           func(bool) error
}
