package psimpl

import (
	"github.com/couchbase/stellar-nebula/genproto/admin_bucket_v1"
	"github.com/couchbase/stellar-nebula/genproto/analytics_v1"
	"github.com/couchbase/stellar-nebula/genproto/couchbase_v1"
	"github.com/couchbase/stellar-nebula/genproto/data_v1"
	"github.com/couchbase/stellar-nebula/genproto/query_v1"
	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
	"github.com/couchbase/stellar-nebula/genproto/search_v1"
	"github.com/couchbase/stellar-nebula/genproto/transactions_v1"
)

type Impl interface {
	CouchbaseV1() couchbase_v1.CouchbaseServer
	RoutingV1() routing_v1.RoutingServer
	DataV1() data_v1.DataServer
	QueryV1() query_v1.QueryServer
	SearchV1() search_v1.SearchServer
	AnalyticsV1() analytics_v1.AnalyticsServer
	AdminBucketV1() admin_bucket_v1.BucketAdminServer
	TransactionsV1() transactions_v1.TransactionsServer
}
