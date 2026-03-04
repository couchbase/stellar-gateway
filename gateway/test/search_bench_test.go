package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/admin_search_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/search_v1"
	"github.com/couchbase/stellar-gateway/contrib/grpcheaderauth"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/couchbase/stellar-gateway/utils/selfsignedcert"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type searchBenchEnv struct {
	runID string

	cluster *testutils.CanonicalTestCluster

	createSg bool
	gwCancel context.CancelFunc
	gwClosed chan struct{}

	connAddr  string
	conn      *grpc.ClientConn
	kvCli     kv_v1.KvServiceClient
	searchCli search_v1.SearchServiceClient
	adminCli  admin_search_v1.SearchAdminServiceClient
	creds     grpc.CallOption

	bucket     string
	scope      string
	collection string

	indexName      string
	benchDocIDs    []string
	smallBenchDocs []string
}

var (
	searchBenchEnvOnce        sync.Once
	searchBenchEnvCleanupOnce sync.Once
	searchBenchEnvInst        *searchBenchEnv
	searchBenchEnvErr         error
)

func getSearchBenchEnv(tb testing.TB) *searchBenchEnv {
	tb.Helper()

	searchBenchEnvOnce.Do(func() {
		env, err := newSearchBenchEnv(tb)
		searchBenchEnvInst = env
		searchBenchEnvErr = err
	})

	if searchBenchEnvErr != nil {
		tb.Fatalf("skipping search benchmarks: %v", searchBenchEnvErr)
	}

	return searchBenchEnvInst
}

func newSearchBenchEnv(tb testing.TB) (*searchBenchEnv, error) {
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

	env := &searchBenchEnv{
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
		"search-bench-client",
	)
	if err != nil {
		_ = env.Close()
		return nil, fmt.Errorf("failed to start/connect gateway: %w", err)
	}

	env.connAddr = gwe.gwConnAddr
	env.conn = gwe.gatewayConn
	env.kvCli = kv_v1.NewKvServiceClient(gwe.gatewayConn)
	env.searchCli = search_v1.NewSearchServiceClient(gwe.gatewayConn)
	env.adminCli = admin_search_v1.NewSearchAdminServiceClient(gwe.gatewayConn)
	env.gwCancel = gwe.gatewayCloseFunc
	env.gwClosed = gwe.gatewayClosedCh

	if err := env.preflightGatewayReady(); err != nil {
		_ = env.Close()
		return nil, err
	}

	if err := env.setupSearchIndexAndDocs(); err != nil {
		_ = env.Close()
		return nil, err
	}

	return env, nil
}

func (e *searchBenchEnv) preflightGatewayReady() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := fmt.Sprintf("search-bench-%s-preflight", e.runID)
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
		_, err := e.kvCli.Upsert(ctx, req, e.creds)
		if err == nil {
			_, _ = e.kvCli.Remove(context.Background(), &kv_v1.RemoveRequest{
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

func (e *searchBenchEnv) setupSearchIndexAndDocs() error {
	// Insert enough documents to trigger the row batching logic (MaxRowBytes) when
	// fields/highlights/locations are enabled.
	const numDocs = 250

	countries := []string{"norway", "belgium", "states", "united"}
	ids := make([]string, 0, numDocs)

	ctx := context.Background()
	for i := 0; i < numDocs; i++ {
		id := fmt.Sprintf("search-bench-%s-%d", e.runID, i)
		ids = append(ids, id)

		doc := map[string]any{
			"type":        "searchbench",
			"service":     "search",
			"country":     countries[i%len(countries)],
			"capacity":    50 + i,
			"updated":     time.Date(2010+(i%10), 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
			"description": fmt.Sprintf("search bench document %d - this is some repeated text to inflate payload size", i),
		}

		content, err := json.Marshal(doc)
		if err != nil {
			return err
		}

		_, err = e.kvCli.Upsert(ctx, &kv_v1.UpsertRequest{
			BucketName:     e.bucket,
			ScopeName:      e.scope,
			CollectionName: e.collection,
			Key:            id,
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: content,
			},
			ContentFlags: benchFlagsJSON,
		}, e.creds)
		if err != nil {
			return fmt.Errorf("failed to upsert bench doc: %w", err)
		}
	}

	e.benchDocIDs = ids
	if len(ids) > 10 {
		e.smallBenchDocs = ids[:10]
	} else {
		e.smallBenchDocs = ids
	}

	idxName := "a" + uuid.NewString()[:6]
	e.indexName = idxName

	sourceType := "couchbase"
	_, err := e.adminCli.CreateIndex(ctx, &admin_search_v1.CreateIndexRequest{
		Name:       idxName,
		SourceName: &e.bucket,
		SourceType: &sourceType,
		Type:       "fulltext-index",
		Params: map[string][]byte{
			"mapping": []byte(`{
				"default_analyzer": "standard",
				"default_datetime_parser": "dateTimeOptional",
				"default_field": "_all",
				"default_mapping": {
					"dynamic": true,
					"enabled": true
				}
			}`),
		},
	}, e.creds)
	if err != nil {
		return fmt.Errorf("failed to create search index (search may be disabled): %w", err)
	}

	// Wait until the index has ingested our documents.
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		ok, err := e.tryDocIDQuery(len(e.benchDocIDs))
		if err == nil && ok {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("search index did not become ready within timeout")
}

func (e *searchBenchEnv) tryDocIDQuery(expectedHits int) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := &search_v1.Query{
		Query: &search_v1.Query_DocIdQuery{
			DocIdQuery: &search_v1.DocIdQuery{Ids: e.benchDocIDs},
		},
	}

	stream, err := e.searchCli.SearchQuery(ctx, &search_v1.SearchQueryRequest{
		IndexName: e.indexName,
		Query:     query,
		Limit:     uint32(expectedHits),
	}, e.creds)
	if err != nil {
		return false, err
	}

	numHits := 0
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return false, err
		}
		numHits += len(res.Hits)
	}

	return numHits == expectedHits, nil
}

func (e *searchBenchEnv) Close() error {
	if e.indexName != "" && e.adminCli != nil {
		_, _ = e.adminCli.DeleteIndex(context.Background(), &admin_search_v1.DeleteIndexRequest{
			Name: e.indexName,
		}, e.creds)
		e.indexName = ""
	}

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

func BenchmarkSearchMatchAll(b *testing.B) {
	e := getSearchBenchEnv(b)
	ctx := context.Background()

	matchAllQuery := &search_v1.Query_MatchAllQuery{MatchAllQuery: &search_v1.MatchAllQuery{}}
	docIDQuery := &search_v1.Query_DocIdQuery{DocIdQuery: &search_v1.DocIdQuery{Ids: e.smallBenchDocs}}

	query := &search_v1.Query{
		Query: &search_v1.Query_ConjunctionQuery{
			ConjunctionQuery: &search_v1.ConjunctionQuery{
				Queries: []*search_v1.Query{{Query: matchAllQuery}, {Query: docIDQuery}},
			},
		},
	}

	req := &search_v1.SearchQueryRequest{
		IndexName: e.indexName,
		Query:     query,
		Limit:     uint32(len(e.smallBenchDocs)),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := e.searchCli.SearchQuery(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("search failed: %v", err)
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("search stream error: %v", err)
			}
		}
	}
}

func BenchmarkSearchMatchNone(b *testing.B) {
	e := getSearchBenchEnv(b)
	ctx := context.Background()

	query := &search_v1.Query{
		Query: &search_v1.Query_MatchNoneQuery{MatchNoneQuery: &search_v1.MatchNoneQuery{}},
	}

	req := &search_v1.SearchQueryRequest{
		IndexName: e.indexName,
		Query:     query,
		Limit:     0,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := e.searchCli.SearchQuery(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("search failed: %v", err)
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("search stream error: %v", err)
			}
		}
	}
}

func BenchmarkSearchLargeResultSet(b *testing.B) {
	e := getSearchBenchEnv(b)
	ctx := context.Background()

	query := &search_v1.Query{
		Query: &search_v1.Query_DocIdQuery{
			DocIdQuery: &search_v1.DocIdQuery{Ids: e.benchDocIDs},
		},
	}

	req := &search_v1.SearchQueryRequest{
		IndexName: e.indexName,
		Query:     query,
		Limit:     uint32(len(e.benchDocIDs)),
		Fields:    []string{"country", "capacity", "updated", "description"},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := e.searchCli.SearchQuery(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("search failed: %v", err)
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("search stream error: %v", err)
			}
		}
	}
}

func BenchmarkSearchWithFacets(b *testing.B) {
	e := getSearchBenchEnv(b)
	ctx := context.Background()

	matchAllQuery := &search_v1.Query_MatchAllQuery{MatchAllQuery: &search_v1.MatchAllQuery{}}
	docIDQuery := &search_v1.Query_DocIdQuery{DocIdQuery: &search_v1.DocIdQuery{Ids: e.smallBenchDocs}}

	query := &search_v1.Query{
		Query: &search_v1.Query_ConjunctionQuery{
			ConjunctionQuery: &search_v1.ConjunctionQuery{
				Queries: []*search_v1.Query{{Query: matchAllQuery}, {Query: docIDQuery}},
			},
		},
	}

	min := float32(0)
	max := float32(5000)
	start := "2000-01-01T00:00:00Z"
	end := "2030-01-01T00:00:00Z"

	req := &search_v1.SearchQueryRequest{
		IndexName:        e.indexName,
		Query:            query,
		Limit:            uint32(len(e.smallBenchDocs)),
		IncludeLocations: true,
		HighlightFields:  []string{"description"},
		Facets: map[string]*search_v1.Facet{
			"country": {
				Facet: &search_v1.Facet_TermFacet{
					TermFacet: &search_v1.TermFacet{Field: "country", Size: 4},
				},
			},
			"capacity": {
				Facet: &search_v1.Facet_NumericRangeFacet{
					NumericRangeFacet: &search_v1.NumericRangeFacet{
						Field: "capacity",
						Size:  2,
						NumericRanges: []*search_v1.NumericRange{{
							Name: "all",
							Min:  &min,
							Max:  &max,
						}},
					},
				},
			},
			"updated": {
				Facet: &search_v1.Facet_DateRangeFacet{
					DateRangeFacet: &search_v1.DateRangeFacet{
						Field: "updated",
						Size:  1,
						DateRanges: []*search_v1.DateRange{{
							Name:  "all",
							Start: &start,
							End:   &end,
						}},
					},
				},
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := e.searchCli.SearchQuery(ctx, req, e.creds)
		if err != nil {
			b.Fatalf("search failed: %v", err)
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("search stream error: %v", err)
			}
		}
	}
}
