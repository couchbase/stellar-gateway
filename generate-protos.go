//go:generate rm -rf ./contrib/goprotostellar/genproto
//go:generate mkdir -p ./contrib/goprotostellar/genproto
//go:generate -command protostellar protoc --proto_path=./contrib/protostellar --proto_path=./contrib/googleapis --go_out=./contrib/goprotostellar/genproto --go_opt=module=github.com/couchbase/goprotostellar/genproto --go-grpc_out=./contrib/goprotostellar/genproto --go-grpc_opt=module=github.com/couchbase/goprotostellar/genproto
//go:generate protostellar couchbase/kv/v1/kv.proto
//go:generate protostellar couchbase/query/v1/query.proto
//go:generate protostellar couchbase/search/v1/search.proto
//go:generate protostellar couchbase/analytics/v1/analytics.proto
//go:generate protostellar couchbase/view/v1/view.proto
//go:generate protostellar couchbase/transactions/v1/transactions.proto
//go:generate protostellar couchbase/routing/v1/routing.proto
//go:generate protostellar couchbase/admin/bucket/v1/bucket.proto
//go:generate protostellar couchbase/admin/collection/v1/collection.proto
//go:generate protostellar couchbase/admin/searchindex/v1/searchindex.proto
//go:generate protostellar couchbase/internal/hooks/v1/hooks.proto

package main
