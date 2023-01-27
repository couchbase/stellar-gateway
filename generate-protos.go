//go:generate mkdir -p ./genproto
//go:generate -command protostellar protoc --proto_path=./proto --proto_path=./contrib/googleapis --go_out=./genproto --go_opt=module=github.com/couchbase/stellar-nebula/genproto --go-grpc_out=./genproto --go-grpc_opt=module=github.com/couchbase/stellar-nebula/genproto
//go:generate protostellar couchbase/kv.v1.proto
//go:generate protostellar couchbase/query.v1.proto
//go:generate protostellar couchbase/search.v1.proto
//go:generate protostellar couchbase/analytics.v1.proto
//go:generate protostellar couchbase/view.v1.proto
//go:generate protostellar couchbase/transactions.v1.proto
//go:generate protostellar couchbase/routing.v1.proto
//go:generate protostellar couchbase/admin/bucket.v1.proto
//go:generate protostellar couchbase/admin/collection.v1.proto
//go:generate protostellar couchbase/internal/hooks.v1.proto
//go:generate protostellar couchbase/internal/health.v1.proto

package main
