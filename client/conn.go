package client

import (
	data_v1 "github.com/couchbase/stellar-nebula/genproto/data/v1"
	query_v1 "github.com/couchbase/stellar-nebula/genproto/query/v1"
	routing_v1 "github.com/couchbase/stellar-nebula/genproto/routing/v1"
)

type Conn interface {
	RoutingV1() routing_v1.RoutingClient
	DataV1() data_v1.DataClient
	QueryV1() query_v1.QueryClient
}
