package test

import (
	"context"
	"sync"
	"time"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// This test needs to block traffic to all but one of the cluster nodes. This can cause the failures of other tests when
// run in paralell so this is hidden behind build tags so we can run them separately.
func (s *GatewayOpsTestSuite) TestSyncWriteAmbiguous() {
	testutils.SkipIfNoDinoCluster(s.T())
	dino := testutils.StartDinoTesting(s.T(), true)

	nodes := testutils.GetTestNodes(s.T())
	for _, node := range nodes {
		if !node.IsOrchestrator {
			dino.BlockNodeTraffic(node.Hostname)
		}
	}

	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	type testCase struct {
		name string
		fn   func(context.Context) error
	}

	tests := []testCase{
		{
			name: "Insert",
			fn: func(ctx context.Context) error {
				docId := s.randomDocId()
				_, err := kvClient.Insert(ctx, &kv_v1.InsertRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					Content: &kv_v1.InsertRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
					ContentFlags:    TEST_CONTENT_FLAGS,
					DurabilityLevel: kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY.Enum(),
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				return err
			},
		},
		{
			name: "Upsert",
			fn: func(ctx context.Context) error {
				docId := s.randomDocId()
				_, err := kvClient.Upsert(ctx, &kv_v1.UpsertRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					Content: &kv_v1.UpsertRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
					ContentFlags:    TEST_CONTENT_FLAGS,
					DurabilityLevel: kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY.Enum(),
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				return err
			},
		},
		{
			name: "Replace",
			fn: func(ctx context.Context) error {
				docId := s.testDocId()
				_, err := kvClient.Replace(ctx, &kv_v1.ReplaceRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					Content: &kv_v1.ReplaceRequest_ContentUncompressed{
						ContentUncompressed: TEST_CONTENT,
					},
					ContentFlags:    TEST_CONTENT_FLAGS,
					DurabilityLevel: kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY.Enum(),
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				return err
			},
		},
		{
			name: "Remove",
			fn: func(ctx context.Context) error {
				docId := s.testDocId()
				_, err := kvClient.Remove(ctx, &kv_v1.RemoveRequest{
					BucketName:      s.bucketName,
					ScopeName:       s.scopeName,
					CollectionName:  s.collectionName,
					Key:             docId,
					DurabilityLevel: kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY.Enum(),
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				return err
			},
		},
		{
			name: "Increment",
			fn: func(ctx context.Context) error {
				docId := s.binaryDocId([]byte("5"))
				_, err := kvClient.Increment(ctx, &kv_v1.IncrementRequest{
					BucketName:      s.bucketName,
					ScopeName:       s.scopeName,
					CollectionName:  s.collectionName,
					Key:             docId,
					Delta:           1,
					DurabilityLevel: kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY.Enum(),
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				return err
			},
		},
		{
			name: "Decrement",
			fn: func(ctx context.Context) error {
				docId := s.binaryDocId([]byte("5"))
				_, err := kvClient.Decrement(ctx, &kv_v1.DecrementRequest{
					BucketName:      s.bucketName,
					ScopeName:       s.scopeName,
					CollectionName:  s.collectionName,
					Key:             docId,
					Delta:           1,
					DurabilityLevel: kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY.Enum(),
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				return err
			},
		},
		{
			name: "Append",
			fn: func(ctx context.Context) error {
				docId := s.binaryDocId([]byte("hello"))
				_, err := kvClient.Append(ctx, &kv_v1.AppendRequest{
					BucketName:      s.bucketName,
					ScopeName:       s.scopeName,
					CollectionName:  s.collectionName,
					Key:             docId,
					Content:         []byte("world"),
					DurabilityLevel: kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY.Enum(),
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				return err
			},
		},
		{
			name: "Prepend",
			fn: func(ctx context.Context) error {
				docId := s.binaryDocId([]byte("hello"))
				_, err := kvClient.Prepend(ctx, &kv_v1.PrependRequest{
					BucketName:      s.bucketName,
					ScopeName:       s.scopeName,
					CollectionName:  s.collectionName,
					Key:             docId,
					Content:         []byte("world"),
					DurabilityLevel: kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY.Enum(),
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				return err
			},
		},
		{
			name: "MutateIn",
			fn: func(ctx context.Context) error {
				docId := s.binaryDocId([]byte(`{"foo": 14}`))
				_, err := kvClient.MutateIn(ctx, &kv_v1.MutateInRequest{
					BucketName:      s.bucketName,
					ScopeName:       s.scopeName,
					CollectionName:  s.collectionName,
					Key:             docId,
					DurabilityLevel: kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY.Enum(),
					Specs: []*kv_v1.MutateInRequest_Spec{{
						Operation: kv_v1.MutateInRequest_Spec_OPERATION_INSERT,
						Path:      "newfoo",
						Content:   []byte(`"baz"`),
					}},
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				return err
			},
		},
	}

	var wg sync.WaitGroup
	for _, tc := range tests {
		wg.Add(1)
		go func() {
			s.Run(tc.name, func() {
				defer wg.Done()
				require.Eventually(s.T(), func() bool {
					// Timeout must be greater that the server sync write timeout (30 seconds) else STG will cancel the
					// operation before we can observe sync write ambiguous.
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*40)
					defer cancel()
					err := tc.fn(ctx)

					if err != nil {
						assertRpcStatus(s.T(), err, codes.DeadlineExceeded)
						assert.Contains(s.T(), err.Error(), "Sync write operation")
						return true
					}

					return false
				}, 60*time.Second, 5*time.Second)
			})
		}()
	}
	wg.Wait()
}
