package queue_test

import (
	"testing"

	"github.com/breez/lightninglib/chainntnfs"
	"github.com/breez/lightninglib/queue"
)

func TestConcurrentQueue(t *testing.T) {
	queue := queue.NewConcurrentQueue(100)
	queue.Start()
	defer queue.Stop()

	// Pushes should never block for long.
	for i := 0; i < 1000; i++ {
		queue.ChanIn() <- i
	}

	// Pops also should not block for long. Expect elements in FIFO order.
	for i := 0; i < 1000; i++ {
		item := <-queue.ChanOut()
		if i != item.(int) {
			t.Fatalf("Dequeued wrong value: expected %d, got %d", i, item.(int))
		}
	}
}
