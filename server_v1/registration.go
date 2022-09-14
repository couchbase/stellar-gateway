package server_v1

import (
	"github.com/couchbase/gocb/v2"
	admin_bucket_v1 "github.com/couchbase/stellar-nebula/genproto/admin/bucket/v1"
	analytics_v1 "github.com/couchbase/stellar-nebula/genproto/analytics/v1"
	data_v1 "github.com/couchbase/stellar-nebula/genproto/data/v1"
	query_v1 "github.com/couchbase/stellar-nebula/genproto/query/v1"
	routing_v1 "github.com/couchbase/stellar-nebula/genproto/routing/v1"
	search_v1 "github.com/couchbase/stellar-nebula/genproto/search/v1"
	transactions_v1 "github.com/couchbase/stellar-nebula/genproto/transactions/v1"
	couchbase_v1 "github.com/couchbase/stellar-nebula/genproto/v1"
	"github.com/couchbase/stellar-nebula/topology"
	"google.golang.org/grpc"
)

func Register(s *grpc.Server, topologyManager *topology.TopologyManager, client *gocb.Cluster) {
	couchbase_v1.RegisterCouchbaseServer(s, NewCouchbaseServer())
	routing_v1.RegisterRoutingServer(s, NewRoutingServer(topologyManager))
	data_v1.RegisterDataServer(s, NewDataServer(client))
	query_v1.RegisterQueryServer(s, NewQueryServer(client))
	search_v1.RegisterSearchServer(s, NewSearchServer(client))
	analytics_v1.RegisterAnalyticsServer(s, NewAnalyticsServer(client))
	admin_bucket_v1.RegisterBucketAdminServer(s, NewBucketAdminServer(client))
	transactions_v1.RegisterTransactionsServer(s, NewTransactionsServer(client))
}
