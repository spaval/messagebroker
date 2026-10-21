package rabbitmq

import "sync"

type Subject interface {
	Register(observer Observer)
	NotifyAll()
}

type Observer interface {
	GetID() string
	Update()
}

type RabbitObserver struct {
	// lock guards observers. Register runs while the application wires its
	// consumers up and NotifyAll runs from whichever goroutine reconnected, so
	// without it the two race on the same slice.
	lock      sync.RWMutex
	observers []Observer
}

func NewRabbitObserver() *RabbitObserver {
	return &RabbitObserver{}
}

// NotifyAll tells every registered observer to subscribe again. It copies the
// slice first so an observer that registers another one from its Update does
// not mutate the list this loop is walking.
func (e *RabbitObserver) NotifyAll() {
	e.lock.RLock()
	observers := make([]Observer, len(e.observers))
	copy(observers, e.observers)
	e.lock.RUnlock()

	for _, observer := range observers {
		observer.Update()
	}
}

func (e *RabbitObserver) Register(observer Observer) {
	e.lock.Lock()
	defer e.lock.Unlock()

	e.observers = append(e.observers, observer)
}
