/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package client

import "sync/atomic"

type routingEndpoint struct {
	Address string
}

type dataRoutingEndpoint struct {
	Address       string
	LocalVbuckets []int
	GroupVbuckets []int
}

type bucketRoutingTable struct {
	Endpoints []*dataRoutingEndpoint
}

type routingTable struct {
	Conns     []*routingConn
	Endpoints []*routingEndpoint

	Buckets map[string]*bucketRoutingTable
}

type atomicRoutingTable struct {
	Value atomic.Value
}

func (t *atomicRoutingTable) Load() *routingTable {
	return t.Value.Load().(*routingTable)
}

func (t *atomicRoutingTable) Store(new *routingTable) {
	t.Value.Store(new)
}

func (t *atomicRoutingTable) CompareAndSwap(old, new *routingTable) bool {
	return t.Value.CompareAndSwap(old, new)
}
