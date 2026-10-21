package rabbitmq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingObserver records how many times it was told to subscribe again.
type countingObserver struct {
	id      string
	updates atomic.Int64
}

func (o *countingObserver) GetID() string { return o.id }
func (o *countingObserver) Update()       { o.updates.Add(1) }

// registeringObserver registers another observer from its own Update, the way a
// consumer that builds a nested subscription would.
type registeringObserver struct {
	subject *RabbitObserver
}

func (o *registeringObserver) GetID() string { return "registering" }
func (o *registeringObserver) Update() {
	o.subject.Register(&countingObserver{id: "nested"})
}

func TestNotifyAllReachesEveryRegisteredObserver(t *testing.T) {
	subject := NewRabbitObserver()
	first := &countingObserver{id: "first"}
	second := &countingObserver{id: "second"}

	subject.Register(first)
	subject.Register(second)
	subject.NotifyAll()

	if first.updates.Load() != 1 || second.updates.Load() != 1 {
		t.Errorf("updates: got %d and %d, expected 1 each", first.updates.Load(), second.updates.Load())
	}
}

// NotifyAll walks a copy of the slice, so an observer is free to register
// another one from its Update. Walking the slice under the read lock instead
// would deadlock here, because Register waits for the write lock.
func TestNotifyAllAllowsAnObserverToRegisterAnother(t *testing.T) {
	subject := NewRabbitObserver()
	subject.Register(&registeringObserver{subject: subject})

	done := make(chan struct{})

	go func() {
		subject.NotifyAll()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("NotifyAll deadlocked against Register")
	}
}

// Register runs while the application wires its consumers up and NotifyAll runs
// from whichever goroutine reconnected. Under -race this fails if the slice is
// left unguarded.
func TestRegisterAndNotifyAllAreSafeConcurrently(t *testing.T) {
	subject := NewRabbitObserver()

	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(2)

		go func(n int) {
			defer wg.Done()
			subject.Register(&countingObserver{id: fmt.Sprintf("observer-%d", n)})
		}(i)

		go func() {
			defer wg.Done()
			subject.NotifyAll()
		}()
	}

	wg.Wait()
}
