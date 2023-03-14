package client

import (
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/couchbase/goprotostellar/genproto/routing_v1"
)

type Conn interface {
	RoutingV1() routing_v1.RoutingServiceClient
	KvV1() kv_v1.KvServiceClient
	QueryV1() query_v1.QueryServiceClient
}
