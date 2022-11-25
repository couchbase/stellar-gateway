//go:generate mkdir -p ./genproto
//go:generate -command protostellar protoc --go_out=./genproto --go_opt=module=github.com/couchbase/stellar-nebula/genproto --go-grpc_out=./genproto --go-grpc_opt=module=github.com/couchbase/stellar-nebula/genproto
//go:generate protostellar proto/com.couchbase.v1.proto
//go:generate protostellar proto/com.couchbase.kv.v1.proto
//go:generate protostellar proto/com.couchbase.query.v1.proto
//go:generate protostellar proto/com.couchbase.search.v1.proto
//go:generate protostellar proto/com.couchbase.analytics.v1.proto
//go:generate protostellar proto/com.couchbase.view.v1.proto
//go:generate protostellar proto/com.couchbase.transactions.v1.proto
//go:generate protostellar proto/com.couchbase.routing.v1.proto
//go:generate protostellar proto/com.couchbase.admin.bucket.v1.proto
//go:generate protostellar proto/com.couchbase.internal.hooks.v1.proto

package main
