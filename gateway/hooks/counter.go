package hooks

import (
	"context"
	"sync"

	"golang.org/x/exp/slices"
)

type counterWatcher struct {
	Ctx context.Context
	Ch  chan<- int64
}

type Counter struct {
	lock     sync.Mutex
	value    int64
	watchers []*counterWatcher
}

func newCounter() *Counter {
	return &Counter{}
}

func (c *Counter) Get() int64 {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.value
}

func (c *Counter) Update(delta int64) int64 {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.value += delta

	newValue := c.value
	for _, watcher := range c.watchers {
		select {
		case watcher.Ch <- newValue:
		case <-watcher.Ctx.Done():
		}
	}

	return newValue
}

func (c *Counter) Watch(ctx context.Context) <-chan int64 {
	outputCh := make(chan int64)

	// start a goroutine to marshal the data through to the output stream so that
	// we can ensure that the initial value is correctly ordered on the channel.
	go func() {
		internalCh := make(chan int64)

		watcher := &counterWatcher{
			Ctx: ctx,
			Ch:  internalCh,
		}

		c.lock.Lock()
		c.watchers = append(c.watchers, watcher)
		initialValue := c.value
		c.lock.Unlock()

		value := initialValue

	MainLoop:
		for {
			// we do a send first to send the initial value
			select {
			case outputCh <- value:
			case <-ctx.Done():
				break MainLoop
			}

			// grab the next value every time we are done
			select {
			case newValue := <-internalCh:
				value = newValue
			case <-ctx.Done():
				break MainLoop
			}
		}

		c.lock.Lock()
		watcherIdx := slices.Index(c.watchers, watcher)
		if watcherIdx >= 0 {
			c.watchers[watcherIdx] = c.watchers[len(c.watchers)-1]
			c.watchers = c.watchers[:len(c.watchers)-1]
		}
		c.lock.Unlock()
	}()

	return outputCh
}
