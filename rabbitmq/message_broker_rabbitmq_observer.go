package rabbitmq

type Subject interface {
	Register(observer Observer)
	NotifyAll()
}

type Observer interface {
	GetID() string
	Update()
}

type RabbitObserver struct {
	observers []Observer
}

func NewRabbitObserver() *RabbitObserver {
	return &RabbitObserver{}
}

func (e *RabbitObserver) NotifyAll() {
	for _, observer := range e.observers {
		observer.Update()
	}
}

func (e *RabbitObserver) Register(observer Observer) {
	e.observers = append(e.observers, observer)
}
