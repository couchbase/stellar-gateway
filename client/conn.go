package client

import (
	"github.com/couchbase/stellar-nebula/genproto/kv_v1"
	"github.com/couchbase/stellar-nebula/genproto/query_v1"
	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
)

type Conn interface {
	RoutingV1() routing_v1.RoutingClient
	KvV1() kv_v1.KvClient
	QueryV1() query_v1.QueryClient
}
