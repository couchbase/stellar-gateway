package test

import (
	"context"
	"time"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// This test needs to remove all but one of the cluster nodes. Therefore this test will put the cluster in a bad state and
// must be run at the end or against an isolated cluster.
func (s *GatewayOpsTestSuite) TestDurabilityImpossible() {
	testutils.SkipIfNoDinoCluster(s.T())
	dino := testutils.StartDinoTesting(s.T(), false)

	nodes := testutils.GetTestNodes(s.T())
	for _, node := range nodes {
		if !node.IsOrchestrator {
			dino.RemoveNode(node.Hostname)
		}
	}

	// BUG(ING-1294) - ops can hang when performed immediately after node removal
	time.Sleep(time.Second * 10)

	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	type testCase struct {
		name string
		fn   func() error
	}

	tests := []testCase{
		{
			name: "Insert",
			fn: func() error {
				docId := s.randomDocId()
				_, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
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
			fn: func() error {
				docId := s.randomDocId()
				_, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
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
			fn: func() error {
				docId := s.testDocId()
				_, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
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
			fn: func() error {
				docId := s.testDocId()
				_, err := kvClient.Remove(context.Background(), &kv_v1.RemoveRequest{
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
			fn: func() error {
				docId := s.binaryDocId([]byte("5"))
				_, err := kvClient.Increment(context.Background(), &kv_v1.IncrementRequest{
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
			fn: func() error {
				docId := s.binaryDocId([]byte("5"))
				_, err := kvClient.Decrement(context.Background(), &kv_v1.DecrementRequest{
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
			fn: func() error {
				docId := s.binaryDocId([]byte("hello"))
				_, err := kvClient.Append(context.Background(), &kv_v1.AppendRequest{
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
			fn: func() error {
				docId := s.binaryDocId([]byte("hello"))
				_, err := kvClient.Prepend(context.Background(), &kv_v1.PrependRequest{
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
			fn: func() error {
				docId := s.binaryDocId([]byte(`{"foo": 14}`))
				_, err := kvClient.MutateIn(context.Background(), &kv_v1.MutateInRequest{
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

	for _, tc := range tests {
		s.Run(tc.name, func() {
			err := tc.fn()
			assertRpcStatus(s.T(), err, codes.FailedPrecondition)
			assert.Contains(s.T(), err.Error(), "Not enough servers to use this durability level")
		})
	}
}
