/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package client

import (
	"crypto/x509"
	"math/rand"
	"net"
	"sync"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/couchbase/goprotostellar/genproto/routing_v1"
	"go.uber.org/zap"
)

type routingClient_Bucket struct {
	RefCount uint
	Watcher  *routingWatcher
}

type RoutingClient struct {
	routing *atomicRoutingTable
	lock    sync.Mutex
	buckets map[string]*routingClient_Bucket
	logger  *zap.Logger
}

// Verify that RoutingClient implements Conn
var _ Conn = (*RoutingClient)(nil)

type DialOptions struct {
	ClientCertificate *x509.CertPool
	Username          string
	Password          string
	Logger            *zap.Logger
}

func Dial(target string, opts *DialOptions) (*RoutingClient, error) {
	// use port 18091 by default
	{
		_, _, err := net.SplitHostPort(target)
		if err != nil {
			// if we couldn't split the host/port, assume there is no port
			target = target + ":18098"
		}
	}

	conn, err := dialRoutingConn(target, &routingConnOptions{
		ClientCertificate: opts.ClientCertificate,
		Username:          opts.Username,
		Password:          opts.Password,
	})
	if err != nil {
		return nil, err
	}

	routing := &atomicRoutingTable{}
	routing.Store(&routingTable{
		Conns: []*routingConn{conn},
	})

	return &RoutingClient{
		routing: routing,
		buckets: make(map[string]*routingClient_Bucket),
		logger:  opts.Logger,
	}, nil
}

func (c *RoutingClient) OpenBucket(bucketName string) {
	c.lock.Lock()
	bucket := c.buckets[bucketName]
	if bucket != nil {
		bucket.RefCount++
		c.lock.Unlock()
		return
	}

	watcher := newRoutingWatcher(&routingWatcherOptions{
		RoutingClient: nil,
		BucketName:    bucketName,
		RoutingTable:  c.routing,
	})
	c.buckets[bucketName] = &routingClient_Bucket{
		RefCount: 1,
		Watcher:  watcher,
	}

	c.lock.Unlock()
}

func (c *RoutingClient) CloseBucket(bucketName string) {
	c.lock.Lock()
	bucket := c.buckets[bucketName]
	if bucket == nil {
		// doesn't exist, thats weird...
		c.lock.Unlock()
		c.logger.Info("closed an unopened bucket")
		return
	}

	if bucket.RefCount == 0 {
		// this shouldn't be possible...
		c.lock.Unlock()
		c.logger.Info("closed an unreferenced bucket")
		return
	}

	bucket.RefCount--
	if bucket.RefCount > 0 {
		// there are still references, carry on
		c.lock.Unlock()
		return
	}

	bucket.Watcher.Close()
	delete(c.buckets, bucketName)

	c.lock.Unlock()
}

func (c *RoutingClient) fetchConn() *routingConn {
	// TODO(brett19): We should probably be more clever here...
	r := c.routing.Load()
	randConnIdx := rand.Intn(len(r.Conns) - 1)
	return r.Conns[randConnIdx]
}

func (c *RoutingClient) fetchConnForBucket(bucketName string) *routingConn {
	// TODO(brett19): Implement routing of bucket-specific requests
	return c.fetchConn()
}

func (c *RoutingClient) fetchConnForKey(bucketName string, key string) *routingConn {
	// TODO(brett19): Implement routing of key-specific requests.
	return c.fetchConn()
}

func (c *RoutingClient) RoutingV1() routing_v1.RoutingServiceClient {
	return &routingImpl_RoutingV1{c}
}

func (c *RoutingClient) KvV1() kv_v1.KvServiceClient {
	return &routingImpl_KvV1{c}
}

func (c *RoutingClient) QueryV1() query_v1.QueryServiceClient {
	return &routingImpl_QueryV1{c}
}
