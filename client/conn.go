package client

import (
	"github.com/couchbase/stellar-nebula/genproto/data_v1"
	"github.com/couchbase/stellar-nebula/genproto/query_v1"
	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
)

type Conn interface {
	RoutingV1() routing_v1.RoutingClient
	DataV1() data_v1.DataClient
	QueryV1() query_v1.QueryClient
}
