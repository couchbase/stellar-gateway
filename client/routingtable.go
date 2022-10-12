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
