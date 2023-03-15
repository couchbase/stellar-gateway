package hooks

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type BarrierWaiter struct {
	ch chan []byte

	ID       string
	MetaData []byte
}

type barrierWatcher struct {
	Ctx context.Context
	Ch  chan<- *BarrierWaiter
}

type Barrier struct {
	lock     sync.Mutex
	waiters  []*BarrierWaiter
	watchers []*barrierWatcher
	logger   *zap.Logger
}

func newBarrier(logger *zap.Logger) *Barrier {
	return &Barrier{
		logger: logger,
	}
}

func (c *Barrier) Wait(ctx context.Context, waiterID string, metaData []byte) []byte {
	waiter := &BarrierWaiter{
		ch:       make(chan []byte, 1),
		ID:       waiterID,
		MetaData: metaData,
	}

	c.lock.Lock()
	c.waiters = append(c.waiters, waiter)

	for _, watcher := range c.watchers {
		select {
		case watcher.Ch <- waiter:
		case <-watcher.Ctx.Done():
		}
	}
	c.lock.Unlock()

	c.logger.Info("starting barrier wait")

	var respData []byte
	select {
	case respData = <-waiter.ch:
	case <-ctx.Done():
	}

	c.logger.Info("done barrier wait")

	c.lock.Lock()
	foundWaiterIdx := slices.IndexFunc(
		c.waiters,
		func(w *BarrierWaiter) bool { return w.ID == waiterID })

	if foundWaiterIdx != -1 {
		c.waiters = append(c.waiters[:foundWaiterIdx], c.waiters[foundWaiterIdx+1:]...)
	}
	c.lock.Unlock()

	return respData
}

func (c *Barrier) trySignal(waiterID *string, metaData []byte) bool {
	c.lock.Lock()
	waiterIdx := 0

	if len(c.waiters) == 0 {
		waiterIdx = -1
	}

	if waiterID != nil {
		c.logger.Info("trying to signal specific barrier waiter", zap.String("waiterId", *waiterID), zap.Any("barrier", c))
		foundWaiterIdx := slices.IndexFunc(
			c.waiters,
			func(w *BarrierWaiter) bool { return w.ID == *waiterID })

		waiterIdx = foundWaiterIdx
	}

	if waiterIdx == -1 {
		c.lock.Unlock()
		return false
	}

	foundWaiter := c.waiters[waiterIdx]
	c.waiters = append(c.waiters[:waiterIdx], c.waiters[waiterIdx+1:]...)

	c.lock.Unlock()

	c.logger.Info("signaling barrier channel")

	foundWaiter.ch <- metaData
	close(foundWaiter.ch)

	return true
}

func (c *Barrier) TrySignal(waiterID string, metaData []byte) {
	c.trySignal(&waiterID, metaData)
}

func (c *Barrier) TrySignalAny(metaData []byte) {
	c.trySignal(nil, metaData)
}

func (c *Barrier) SignalAll(metaData []byte) {
	for c.trySignal(nil, metaData) {
	}
}

func (c *Barrier) Watch(ctx context.Context) <-chan *BarrierWaiter {
	outputCh := make(chan *BarrierWaiter)

	// start a goroutine to marshal the data through to the output stream so that
	// we can ensure that the initial value is correctly ordered on the channel.
	go func() {
		internalCh := make(chan *BarrierWaiter)

		watcher := &barrierWatcher{
			Ctx: ctx,
			Ch:  internalCh,
		}

		c.lock.Lock()
		c.watchers = append(c.watchers, watcher)

		initialWaiters := make([]*BarrierWaiter, len(c.waiters))
		copy(initialWaiters, c.waiters)
		c.lock.Unlock()

	MainLoop:
		for {
			// we do sends first to send the initial waiters
			for _, waiter := range initialWaiters {
				select {
				case outputCh <- waiter:
				case <-ctx.Done():
					break MainLoop
				}
			}

			// grab the next value
			select {
			case newWaiter := <-internalCh:
				// try to send the value to the watcher
				select {
				case outputCh <- newWaiter:
				case <-ctx.Done():
					break MainLoop
				}
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
