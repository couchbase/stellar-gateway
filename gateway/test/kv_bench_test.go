package test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/stellar-gateway/contrib/grpcheaderauth"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/couchbase/stellar-gateway/utils/selfsignedcert"
	"github.com/couchbaselabs/gocbconnstr/v2"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type kvBenchEnv struct {
	runID string

	cluster *testutils.CanonicalTestCluster

	createSg bool
	gwCancel context.CancelFunc
	gwClosed chan struct{}

	connAddr string
	conn     *grpc.ClientConn
	cli      kv_v1.KvServiceClient
	creds    grpc.CallOption

	bucket     string
	scope      string
	collection string
}

var (
	kvBenchEnvOnce        sync.Once
	kvBenchEnvCleanupOnce sync.Once
	kvBenchEnvInst        *kvBenchEnv
	kvBenchEnvErr         error
)

func getKvBenchEnv(tb testing.TB) *kvBenchEnv {
	tb.Helper()

	kvBenchEnvOnce.Do(func() {
		env, err := newKvBenchEnv(tb)
		kvBenchEnvInst = env
		kvBenchEnvErr = err
	})

	if kvBenchEnvErr != nil {
		tb.Fatalf("skipping kv benchmarks: %v", kvBenchEnvErr)
	}

	return kvBenchEnvInst
}

func TestMain(m *testing.M) {
	code := m.Run()
	kvBenchEnvCleanupOnce.Do(func() {
		if kvBenchEnvInst != nil {
			_ = kvBenchEnvInst.Close()
			kvBenchEnvInst = nil
		}
	})
	queryBenchEnvCleanupOnce.Do(func() {
		if queryBenchEnvInst != nil {
			_ = queryBenchEnvInst.Close()
			queryBenchEnvInst = nil
		}
	})
	searchBenchEnvCleanupOnce.Do(func() {
		if searchBenchEnvInst != nil {
			_ = searchBenchEnvInst.Close()
			searchBenchEnvInst = nil
		}
	})
	os.Exit(code)
}

func newKvBenchEnv(tb testing.TB) (*kvBenchEnv, error) {
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

	env := &kvBenchEnv{
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
		"bench-client",
	)
	if err != nil {
		_ = env.Close()
		return nil, fmt.Errorf("failed to start/connect gateway: %w", err)
	}

	env.connAddr = gwe.gwConnAddr
	env.conn = gwe.gatewayConn
	env.cli = kv_v1.NewKvServiceClient(gwe.gatewayConn)
	env.gwCancel = gwe.gatewayCloseFunc
	env.gwClosed = gwe.gatewayClosedCh

	if err := env.preflightGatewayReady(); err != nil {
		_ = env.Close()
		return nil, err
	}

	return env, nil
}

func preflightCouchbaseReachable(cbConnStr string) error {
	baseSpec, err := gocbconnstr.Parse(cbConnStr)
	if err != nil {
		return fmt.Errorf("failed to parse SGTEST_CBCONNSTR: %w", err)
	}

	spec, err := gocbconnstr.Resolve(baseSpec)
	if err != nil {
		return fmt.Errorf("failed to resolve SGTEST_CBCONNSTR: %w", err)
	}

	// Prefer KV (memd) reachability since that's required for these benchmarks.
	if len(spec.MemdHosts) > 0 {
		h := spec.MemdHosts[0]
		addr := net.JoinHostPort(h.Host, strconv.Itoa(int(h.Port)))
		conn, err := net.DialTimeout("tcp", addr, 750*time.Millisecond)
		if err != nil {
			return fmt.Errorf("couchbase not reachable at %s (set SGTEST_CBCONNSTR): %w", addr, err)
		}
		_ = conn.Close()
		return nil
	}

	// Fallback to HTTP mgmt port if memd hosts were not resolvable.
	if len(spec.HttpHosts) > 0 {
		h := spec.HttpHosts[0]
		addr := net.JoinHostPort(h.Host, strconv.Itoa(int(h.Port)))
		conn, err := net.DialTimeout("tcp", addr, 750*time.Millisecond)
		if err != nil {
			return fmt.Errorf("couchbase not reachable at %s (set SGTEST_CBCONNSTR): %w", addr, err)
		}
		_ = conn.Close()
		return nil
	}

	return fmt.Errorf("SGTEST_CBCONNSTR did not resolve to any hosts")
}

func (e *kvBenchEnv) preflightGatewayReady() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := fmt.Sprintf("bench-%s-preflight", e.runID)
	req := &kv_v1.UpsertRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
		Content: &kv_v1.UpsertRequest_ContentUncompressed{
			ContentUncompressed: []byte("{}"),
		},
		ContentFlags: benchFlagsJSON,
	}

	var lastErr error
	for ctx.Err() == nil {
		_, err := e.cli.Upsert(ctx, req, e.creds)
		if err == nil {
			_, _ = e.cli.Remove(context.Background(), &kv_v1.RemoveRequest{
				BucketName:     e.bucket,
				ScopeName:      e.scope,
				CollectionName: e.collection,
				Key:            key,
			}, e.creds)
			return nil
		}

		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("gateway not ready (check Couchbase + SGTEST_* env): %w", lastErr)
}

func (e *kvBenchEnv) Close() error {
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

func benchKeyPrefix(e *kvBenchEnv, b *testing.B) string {
	name := strings.NewReplacer("/", "-", " ", "-", "=", "-").Replace(b.Name())
	return fmt.Sprintf("bench-%s-%s", e.runID, name)
}

func (e *kvBenchEnv) upsertDoc(ctx context.Context, key string, content []byte, flags uint32) (uint64, error) {
	resp, err := e.cli.Upsert(ctx, &kv_v1.UpsertRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
		Content: &kv_v1.UpsertRequest_ContentUncompressed{
			ContentUncompressed: content,
		},
		ContentFlags: flags,
	}, e.creds)
	if err != nil {
		return 0, err
	}
	return resp.Cas, nil
}

var (
	benchJSONSmall = []byte(`{"foo":"bar","obj":{"num":14,"arr":[2,5,8],"str":"zz"},"num":11,"arr":[3,6,9,12]}`)
	benchFlagsJSON = uint32(0x01000000)
	benchBinSmall  = []byte("0123456789abcdef")
)

func BenchmarkKVGetSmallJSON(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	key := benchKeyPrefix(e, b) + "-doc"
	_, err := e.upsertDoc(ctx, key, benchJSONSmall, benchFlagsJSON)
	if err != nil {
		b.Fatalf("setup upsert failed: %v", err)
	}

	req := &kv_v1.GetRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.cli.Get(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("get failed: %v", err)
		}
		_ = resp.Cas
	}
}

func BenchmarkKVGetLargeBinary(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	key := benchKeyPrefix(e, b) + "-doc"
	val := make([]byte, 256*1024)
	for i := range val {
		val[i] = byte(i)
	}

	_, err := e.upsertDoc(ctx, key, val, 0)
	if err != nil {
		b.Fatalf("setup upsert failed: %v", err)
	}

	req := &kv_v1.GetRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.cli.Get(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("get failed: %v", err)
		}
		_ = resp.Cas
	}
}

func BenchmarkKVGetAndTouchSmallJSON(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	key := benchKeyPrefix(e, b) + "-doc"
	_, err := e.upsertDoc(ctx, key, benchJSONSmall, benchFlagsJSON)
	if err != nil {
		b.Fatalf("setup upsert failed: %v", err)
	}

	req := &kv_v1.GetAndTouchRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
		Expiry: &kv_v1.GetAndTouchRequest_ExpirySecs{
			ExpirySecs: 60,
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.cli.GetAndTouch(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("getandtouch failed: %v", err)
		}
		_ = resp.Cas
	}
}

func BenchmarkKVGetAndLockSmallJSON(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	req := &kv_v1.GetAndLockRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		LockTimeSecs:   30,
	}

	keys := make([]string, b.N)
	cass := make([]uint64, b.N)
	prefix := benchKeyPrefix(e, b) + "-" + uuid.NewString() + "-doc"
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		keys[i] = key
		_, err := e.upsertDoc(ctx, key, benchJSONSmall, benchFlagsJSON)
		if err != nil {
			b.Fatalf("setup upsert failed: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Key = keys[i]
		resp, err := e.cli.GetAndLock(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("getandlock failed: %v", err)
		}
		cass[i] = resp.Cas
	}
}

func BenchmarkKVUnlock(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	lockReq := &kv_v1.GetAndLockRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		LockTimeSecs:   30,
	}
	unlockReq := &kv_v1.UnlockRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
	}

	keys := make([]string, b.N)
	cass := make([]uint64, b.N)
	prefix := benchKeyPrefix(e, b) + "-" + uuid.NewString() + "-doc"
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		keys[i] = key
		_, err := e.upsertDoc(ctx, key, benchJSONSmall, benchFlagsJSON)
		if err != nil {
			b.Fatalf("setup upsert failed: %v", err)
		}

		lockReq.Key = key
		lockResp, err := e.cli.GetAndLock(ctx, lockReq, e.creds)
		if err != nil {
			b.Fatalf("getandlock (setup) failed: %v", err)
		}
		cass[i] = lockResp.Cas
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		unlockReq.Key = keys[i]
		unlockReq.Cas = cass[i]
		_, err := e.cli.Unlock(ctx, unlockReq, e.creds)
		if err != nil {
			b.Fatalf("unlock failed: %v", err)
		}
	}
}

func BenchmarkKVTouchExpirySecs(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	key := benchKeyPrefix(e, b) + "-doc"
	_, err := e.upsertDoc(ctx, key, benchJSONSmall, benchFlagsJSON)
	if err != nil {
		b.Fatalf("setup upsert failed: %v", err)
	}

	req := &kv_v1.TouchRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
		Expiry: &kv_v1.TouchRequest_ExpirySecs{
			ExpirySecs: 60,
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.cli.Touch(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("touch failed: %v", err)
		}
		_ = resp.Cas
	}
}

func BenchmarkKVInsertSmallJSONUniqueKeys(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()
	// Benchmarks are run multiple times by the harness (calibration), so ensure keys
	// are unique per invocation to avoid AlreadyExists.
	prefix := benchKeyPrefix(e, b) + "-" + uuid.NewString()

	req := &kv_v1.InsertRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Content: &kv_v1.InsertRequest_ContentUncompressed{
			ContentUncompressed: benchJSONSmall,
		},
		ContentFlags: benchFlagsJSON,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Key = fmt.Sprintf("%s-%d", prefix, i)
		resp, err := e.cli.Insert(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("insert failed: %v", err)
		}
		_ = resp.Cas
	}
}

func BenchmarkKVExistsHit(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	key := benchKeyPrefix(e, b) + "-doc"
	_, err := e.upsertDoc(ctx, key, benchJSONSmall, benchFlagsJSON)
	if err != nil {
		b.Fatalf("setup upsert failed: %v", err)
	}

	req := &kv_v1.ExistsRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.cli.Exists(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("exists failed: %v", err)
		}
		_ = resp.Result
	}
}

func BenchmarkKVExistsMiss(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	key := benchKeyPrefix(e, b) + "-missing"
	req := &kv_v1.ExistsRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.cli.Exists(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("exists failed: %v", err)
		}
		_ = resp.Result
	}
}

func BenchmarkKVUpsertSmallJSON(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	key := benchKeyPrefix(e, b) + "-doc"

	req := &kv_v1.UpsertRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
		Content: &kv_v1.UpsertRequest_ContentUncompressed{
			ContentUncompressed: benchJSONSmall,
		},
		ContentFlags: benchFlagsJSON,
	}

	// Ensure the doc exists so the benchmark reflects steady-state overwrite.
	_, err := e.cli.Upsert(ctx, req, e.creds)
	if err != nil {
		b.Fatalf("setup upsert failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.cli.Upsert(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("upsert failed: %v", err)
		}
		_ = resp.Cas
	}
}

func BenchmarkKVReplaceNoCASSmallJSON(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	key := benchKeyPrefix(e, b) + "-doc"
	_, err := e.upsertDoc(ctx, key, benchJSONSmall, benchFlagsJSON)
	if err != nil {
		b.Fatalf("setup upsert failed: %v", err)
	}

	req := &kv_v1.ReplaceRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
		Content: &kv_v1.ReplaceRequest_ContentUncompressed{
			ContentUncompressed: benchJSONSmall,
		},
		ContentFlags: benchFlagsJSON,
		Cas:          nil,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.cli.Replace(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("replace failed: %v", err)
		}
		_ = resp.Cas
	}
}

func BenchmarkKVReplaceWithCASSmallJSON(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	key := benchKeyPrefix(e, b) + "-doc"
	cas, err := e.upsertDoc(ctx, key, benchJSONSmall, benchFlagsJSON)
	if err != nil {
		b.Fatalf("setup upsert failed: %v", err)
	}
	casVar := cas

	req := &kv_v1.ReplaceRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
		Content: &kv_v1.ReplaceRequest_ContentUncompressed{
			ContentUncompressed: benchJSONSmall,
		},
		ContentFlags: benchFlagsJSON,
		Cas:          &casVar,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.cli.Replace(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("replace failed: %v", err)
		}
		casVar = resp.Cas
	}
}

func BenchmarkKVRemoveNoCAS(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()
	prefix := benchKeyPrefix(e, b) + "-" + uuid.NewString()

	req := &kv_v1.RemoveRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Cas:            nil,
	}

	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		keys[i] = key
		_, err := e.upsertDoc(ctx, key, benchJSONSmall, benchFlagsJSON)
		if err != nil {
			b.Fatalf("setup upsert failed: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Key = keys[i]
		resp, err := e.cli.Remove(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("remove failed: %v", err)
		}
		_ = resp.Cas
	}
}

func BenchmarkKVRemoveWithCAS(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()
	prefix := benchKeyPrefix(e, b) + "-" + uuid.NewString()

	casVar := uint64(0)
	req := &kv_v1.RemoveRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Cas:            &casVar,
	}

	keys := make([]string, b.N)
	cass := make([]uint64, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		keys[i] = key
		cas, err := e.upsertDoc(ctx, key, benchJSONSmall, benchFlagsJSON)
		if err != nil {
			b.Fatalf("setup upsert failed: %v", err)
		}
		cass[i] = cas
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Key = keys[i]
		casVar = cass[i]
		resp, err := e.cli.Remove(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("remove failed: %v", err)
		}
		_ = resp.Cas
	}
}

func BenchmarkKVIncrementExistingCounter(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	key := benchKeyPrefix(e, b) + "-counter"
	// Create if missing.
	_, err := e.cli.Increment(ctx, &kv_v1.IncrementRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
		Delta:          1,
		Initial:        ptrToInt64(1_000_000_000_000),
	}, e.creds)
	if err != nil {
		b.Fatalf("setup increment failed: %v", err)
	}

	req := &kv_v1.IncrementRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
		Delta:          1,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.cli.Increment(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("increment failed: %v", err)
		}
		_ = resp.Content
	}
}

func BenchmarkKVDecrementExistingCounter(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	key := benchKeyPrefix(e, b) + "-counter"
	// Create if missing.
	_, err := e.cli.Increment(ctx, &kv_v1.IncrementRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
		Delta:          1,
		Initial:        ptrToInt64(1_000_000_000_000),
	}, e.creds)
	if err != nil {
		b.Fatalf("setup increment failed: %v", err)
	}

	req := &kv_v1.DecrementRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
		Delta:          1,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.cli.Decrement(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("decrement failed: %v", err)
		}
		_ = resp.Content
	}
}

func BenchmarkKVAppendUniqueKeysSmallPayload(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()
	prefix := benchKeyPrefix(e, b) + "-" + uuid.NewString()
	appendPayload := []byte("abcdefghijklmnop")

	req := &kv_v1.AppendRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Content:        appendPayload,
		Cas:            nil,
	}

	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		keys[i] = key
		_, err := e.upsertDoc(ctx, key, benchBinSmall, 0)
		if err != nil {
			b.Fatalf("setup upsert failed: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Key = keys[i]
		resp, err := e.cli.Append(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("append failed: %v", err)
		}
		_ = resp.Cas
	}
}

func BenchmarkKVPrependUniqueKeysSmallPayload(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()
	prefix := benchKeyPrefix(e, b) + "-" + uuid.NewString()
	prependPayload := []byte("abcdefghijklmnop")

	req := &kv_v1.PrependRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Content:        prependPayload,
		Cas:            nil,
	}

	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		keys[i] = key
		_, err := e.upsertDoc(ctx, key, benchBinSmall, 0)
		if err != nil {
			b.Fatalf("setup upsert failed: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req.Key = keys[i]
		resp, err := e.cli.Prepend(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("prepend failed: %v", err)
		}
		_ = resp.Cas
	}
}

func BenchmarkKVLookupIn4Specs(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	key := benchKeyPrefix(e, b) + "-doc"
	_, err := e.upsertDoc(ctx, key, benchJSONSmall, benchFlagsJSON)
	if err != nil {
		b.Fatalf("setup upsert failed: %v", err)
	}

	req := &kv_v1.LookupInRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
		Specs: []*kv_v1.LookupInRequest_Spec{
			{Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET, Path: "foo"},
			{Operation: kv_v1.LookupInRequest_Spec_OPERATION_GET, Path: "obj.num"},
			{Operation: kv_v1.LookupInRequest_Spec_OPERATION_EXISTS, Path: "obj.str"},
			{Operation: kv_v1.LookupInRequest_Spec_OPERATION_COUNT, Path: "arr"},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.cli.LookupIn(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("lookupin failed: %v", err)
		}
		_ = resp.Cas
	}
}

func BenchmarkKVMutateIn4SpecsMixed(b *testing.B) {
	e := getKvBenchEnv(b)
	ctx := context.Background()

	key := benchKeyPrefix(e, b) + "-doc"
	_, err := e.upsertDoc(ctx, key, benchJSONSmall, benchFlagsJSON)
	if err != nil {
		b.Fatalf("setup upsert failed: %v", err)
	}

	req := &kv_v1.MutateInRequest{
		BucketName:     e.bucket,
		ScopeName:      e.scope,
		CollectionName: e.collection,
		Key:            key,
		Specs: []*kv_v1.MutateInRequest_Spec{
			{Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT, Path: "benchA", Content: []byte("1")},
			{Operation: kv_v1.MutateInRequest_Spec_OPERATION_UPSERT, Path: "benchB", Content: []byte("\"x\"")},
			{Operation: kv_v1.MutateInRequest_Spec_OPERATION_REPLACE, Path: "foo", Content: []byte("\"bar\"")},
			{Operation: kv_v1.MutateInRequest_Spec_OPERATION_COUNTER, Path: "benchC", Content: []byte("1")},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := e.cli.MutateIn(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("mutatein failed: %v", err)
		}
		_ = resp.Cas
	}
}

func ptrToInt64(v int64) *int64 {
	return &v
}
