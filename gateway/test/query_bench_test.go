package test

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/couchbase/stellar-gateway/contrib/grpcheaderauth"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/couchbase/stellar-gateway/utils/selfsignedcert"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type queryBenchEnv struct {
	runID string

	cluster *testutils.CanonicalTestCluster

	createSg bool
	gwCancel context.CancelFunc
	gwClosed chan struct{}

	connAddr string
	conn     *grpc.ClientConn
	kvCli    kv_v1.KvServiceClient
	queryCli query_v1.QueryServiceClient
	creds    grpc.CallOption

	bucket     string
	scope      string
	collection string
}

var (
	queryBenchEnvOnce        sync.Once
	queryBenchEnvCleanupOnce sync.Once
	queryBenchEnvInst        *queryBenchEnv
	queryBenchEnvErr         error
)

func getQueryBenchEnv(tb testing.TB) *queryBenchEnv {
	tb.Helper()

	queryBenchEnvOnce.Do(func() {
		env, err := newQueryBenchEnv(tb)
		queryBenchEnvInst = env
		queryBenchEnvErr = err
	})

	if queryBenchEnvErr != nil {
		tb.Fatalf("skipping query benchmarks: %v", queryBenchEnvErr)
	}

	return queryBenchEnvInst
}

// Note: This package already has a TestMain in kv_bench_test.go.
// The query benchmarks reuse the same gateway setup pattern but with their own
// environment (queryBenchEnv) to avoid coupling with the KV benchmark state.

func newQueryBenchEnv(tb testing.TB) (*queryBenchEnv, error) {
	tb.Helper()

	testCfg := testutils.GetTestConfig(tb)

	// Preflight: if Couchbase is not reachable, don't start the gateway only to have it
	// immediately shut down and fail every benchmark.
	if err := preflightCouchbaseReachable(testCfg.CbConnStr); err != nil {
		return nil, err
	}

	cluster, err := testutils.SetupCanonicalTestCluster(testutils.CanonicalTestClusterOptions{
		ConnStr:  testCfg.CbConnStr,
		Username: testCfg.CbUser,
		Password: testCfg.CbPass,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to setup canonical test cluster %w", err)
	}

	creds, err := grpcheaderauth.NewGrpcBasicAuth(cluster.BasicUser, cluster.BasicPass)
	if err != nil {
		_ = cluster.Close()
		return nil, fmt.Errorf("failed to setup grpc basic auth: %w", err)
	}

	env := &queryBenchEnv{
		runID:      uuid.NewString(),
		cluster:    cluster,
		bucket:     cluster.BucketName,
		scope:      cluster.ScopeName,
		collection: cluster.CollectionName,
		creds:      grpc.PerRPCCredentials(creds),
	}

	createSg := testCfg.SgConnStr == ""
	env.createSg = createSg
	if !createSg {
		if testCfg.SgPort == 0 {
			_ = env.Close()
			return nil, fmt.Errorf("SGTEST_SGCONNSTR set but SGTEST_SGPORT is missing or 0")
		}
	}

	var gwLogger *zap.Logger
	if createSg {
		logCfg := zap.NewDevelopmentConfig()
		logCfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
		logger, err := logCfg.Build()
		if err != nil {
			_ = cluster.Close()
			return nil, fmt.Errorf("failed to initialize benchmark logger: %w", err)
		}
		gwLogger = logger.Named("gateway")
	} else {
		gwLogger = zap.NewNop()
	}

	gwCert, err := selfsignedcert.GenerateCertificate()
	if err != nil {
		_ = cluster.Close()
		return nil, fmt.Errorf("failed to create benchmark tls certificate: %w", err)
	}

	gwe, err := startGatewayForTesting(
		context.Background(),
		testCfg,
		gwLogger,
		*gwCert,
		*gwCert,
		nil,
		"query-bench-client",
	)
	if err != nil {
		_ = env.Close()
		return nil, fmt.Errorf("failed to start/connect gateway: %w", err)
	}

	env.connAddr = gwe.gwConnAddr
	env.conn = gwe.gatewayConn
	env.kvCli = kv_v1.NewKvServiceClient(gwe.gatewayConn)
	env.queryCli = query_v1.NewQueryServiceClient(gwe.gatewayConn)
	env.gwCancel = gwe.gatewayCloseFunc
	env.gwClosed = gwe.gatewayClosedCh

	if err := env.preflightGatewayReady(); err != nil {
		_ = env.Close()
		return nil, err
	}

	return env, nil
}

func (e *queryBenchEnv) preflightGatewayReady() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Simple query to verify the query service is working
	stream, err := e.queryCli.Query(ctx, &query_v1.QueryRequest{
		Statement: "SELECT 1",
	}, e.creds)
	if err != nil {
		return fmt.Errorf("query preflight failed: %w", err)
	}

	// Drain the stream
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("query preflight stream error: %w", err)
		}
	}
}

func (e *queryBenchEnv) Close() error {
	if e.conn != nil {
		_ = e.conn.Close()
		e.conn = nil
	}

	if e.createSg && e.gwCancel != nil {
		e.gwCancel()
		if e.gwClosed != nil {
			<-e.gwClosed
		}
		e.gwCancel = nil
		e.gwClosed = nil
	}

	if e.cluster != nil {
		_ = e.cluster.Close()
		e.cluster = nil
	}

	return nil
}

func BenchmarkQuerySimpleSelect(b *testing.B) {
	e := getQueryBenchEnv(b)
	ctx := context.Background()

	req := &query_v1.QueryRequest{
		Statement: "SELECT 1",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := e.queryCli.Query(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		// Drain the stream
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("query stream error: %v", err)
			}
		}
	}
}

func BenchmarkQuerySelectFromBucketSmallResult(b *testing.B) {
	e := getQueryBenchEnv(b)
	ctx := context.Background()

	// Insert a small number of docs for the query to return
	numDocs := 10
	for i := 0; i < numDocs; i++ {
		key := fmt.Sprintf("query-bench-%s-%d", e.runID, i)
		_, err := e.kvCli.Upsert(ctx, &kv_v1.UpsertRequest{
			BucketName:     e.bucket,
			ScopeName:      e.scope,
			CollectionName: e.collection,
			Key:            key,
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: []byte(fmt.Sprintf(`{"id":%d,"value":"test"}`, i)),
			},
			ContentFlags: benchFlagsJSON,
		}, e.creds)
		if err != nil {
			b.Fatalf("setup upsert failed: %v", err)
		}
	}

	req := &query_v1.QueryRequest{
		BucketName: &e.bucket,
		Statement:  fmt.Sprintf("SELECT META().id AS docId, `value` FROM `%s`.`%s`.`%s` WHERE META().id LIKE 'query-bench-%s-%%'", e.bucket, e.scope, e.collection, e.runID),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := e.queryCli.Query(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("query stream error: %v", err)
			}
		}
	}
}

func BenchmarkQuerySelectFromBucketLargeResult(b *testing.B) {
	e := getQueryBenchEnv(b)
	ctx := context.Background()

	// Insert a larger number of docs to stress the row batching logic
	numDocs := 100
	for i := 0; i < numDocs; i++ {
		key := fmt.Sprintf("query-bench-large-%s-%d", e.runID, i)
		_, err := e.kvCli.Upsert(ctx, &kv_v1.UpsertRequest{
			BucketName:     e.bucket,
			ScopeName:      e.scope,
			CollectionName: e.collection,
			Key:            key,
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: []byte(fmt.Sprintf(`{"id":%d,"value":"test-data-payload-%d"}`, i, i)),
			},
			ContentFlags: benchFlagsJSON,
		}, e.creds)
		if err != nil {
			b.Fatalf("setup upsert failed: %v", err)
		}
	}

	req := &query_v1.QueryRequest{
		BucketName: &e.bucket,
		Statement:  fmt.Sprintf("SELECT * FROM `%s`.`%s`.`%s` WHERE META().id LIKE 'query-bench-large-%s-%%'", e.bucket, e.scope, e.collection, e.runID),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := e.queryCli.Query(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("query stream error: %v", err)
			}
		}
	}
}

func BenchmarkQueryWithNamedParameters(b *testing.B) {
	e := getQueryBenchEnv(b)
	ctx := context.Background()

	req := &query_v1.QueryRequest{
		Statement: "SELECT $param",
		NamedParameters: map[string][]byte{
			"param": []byte(`"hello"`),
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := e.queryCli.Query(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("query stream error: %v", err)
			}
		}
	}
}

func BenchmarkQueryWithPositionalParameters(b *testing.B) {
	e := getQueryBenchEnv(b)
	ctx := context.Background()

	req := &query_v1.QueryRequest{
		Statement: "SELECT $1",
		PositionalParameters: [][]byte{
			[]byte(`"hello"`),
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := e.queryCli.Query(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("query stream error: %v", err)
			}
		}
	}
}

func BenchmarkQueryPrepared(b *testing.B) {
	e := getQueryBenchEnv(b)
	ctx := context.Background()

	prepared := true
	req := &query_v1.QueryRequest{
		Statement: "SELECT 1",
		Prepared:  &prepared,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := e.queryCli.Query(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("query stream error: %v", err)
			}
		}
	}
}

func BenchmarkQueryWithScope(b *testing.B) {
	e := getQueryBenchEnv(b)
	ctx := context.Background()

	req := &query_v1.QueryRequest{
		BucketName: &e.bucket,
		ScopeName:  &e.scope,
		Statement:  "SELECT 1",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := e.queryCli.Query(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("query stream error: %v", err)
			}
		}
	}
}
