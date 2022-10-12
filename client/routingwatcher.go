package client

import (
	"context"
	"log"
	"time"

	backoff "github.com/cenkalti/backoff/v4"

	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
)

type routingWatcherOptions struct {
	RoutingClient routing_v1.RoutingClient
	BucketName    string
	RoutingTable  *atomicRoutingTable
}

type routingWatcher struct {
	routingClient routing_v1.RoutingClient
	bucketName    string
	routingTable  *atomicRoutingTable

	ctx       context.Context
	ctxCancel func()
	closeCh   chan struct{}
}

func newRoutingWatcher(opts *routingWatcherOptions) *routingWatcher {
	ctx, ctxCancel := context.WithCancel(context.Background())

	w := &routingWatcher{
		routingClient: opts.RoutingClient,
		bucketName:    opts.BucketName,
		routingTable:  opts.RoutingTable,

		ctx:       ctx,
		ctxCancel: ctxCancel,
		closeCh:   make(chan struct{}),
	}
	w.init()
	return w
}

func (w *routingWatcher) init() {
	go w.procThread()
}

func (w *routingWatcher) procThread() {
	b := backoff.NewExponentialBackOff()
	b.Reset()

MainLoop:
	for {
		topologyCh, err := w.routingClient.WatchRouting(w.ctx, &routing_v1.WatchRoutingRequest{
			BucketName: &w.bucketName,
		})
		if err != nil {
			log.Printf("failed to watch routing: %s", err)

			select {
			case <-time.After(b.NextBackOff()):
				continue
			case <-w.ctx.Done():
				break MainLoop
			}
			// ... handle the error
		}

		// Restart our backoff strategy now that we've successfully started watching...
		b.Reset()

		for {
			topologyData, err := topologyCh.Recv()
			if err != nil {
				log.Printf("failed to recv updated topology: %s", err)
				break
			}

			w.handleTopologyResponse(topologyData)
		}
	}

	close(w.closeCh)
}

func (w *routingWatcher) Close() {
	// shut down our context
	w.ctxCancel()

	// wait for the shutdown to complete
	<-w.closeCh
}

func (w *routingWatcher) handleTopologyResponse(topology *routing_v1.WatchRoutingResponse) {
	// TODO(brett19): Implement handling protostellar topologies received.
}
