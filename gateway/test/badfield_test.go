package test

import (
	"context"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protowire"
)

func (s *GatewayOpsTestSuite) TestUnknownFields() {
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	s.Run("RejectUnknown", func() {
		req := &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
		}

		// craft some unknown fields into the message
		var unkFields []byte
		unkFields = protowire.AppendTag(unkFields, 0xFFFF, protowire.VarintType)
		unkFields = protowire.AppendVarint(unkFields, 0xFF)
		req.ProtoReflect().SetUnknown(unkFields)

		_, err := kvClient.Get(context.Background(), req, grpc.PerRPCCredentials(s.basicRpcCreds))
		assertRpcStatus(s.T(), err, codes.Unimplemented)
	})
}
