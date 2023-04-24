package test

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/stretchr/testify/assert"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *GatewayOpsTestSuite) TestQuery() {
	queryClient := query_v1.NewQueryServiceClient(s.gatewayConn)

	readQueryStream := func(client query_v1.QueryService_QueryClient) ([][]byte, *query_v1.QueryResponse_MetaData, error) {
		var rows [][]byte
		var md *query_v1.QueryResponse_MetaData

		for {
			resp, err := client.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				return rows, md, err
			}

			if len(resp.Rows) > 0 {
				rows = append(rows, resp.Rows...)
			}
			if resp.MetaData != nil {
				md = resp.MetaData
			}
		}

		return rows, md, nil
	}

	s.Run("Basic", func() {
		docId := s.testDocId()

		assert.Eventually(s.T(), func() bool {
			bucketName := "default"
			client, err := queryClient.Query(context.Background(), &query_v1.QueryRequest{
				BucketName: &bucketName,
				Statement:  "SELECT * FROM default._default._default WHERE META().id='" + docId + "'",
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.OK)

			rows, md, err := readQueryStream(client)
			assertRpcStatus(s.T(), err, codes.OK)

			if len(rows) != 1 {
				return false
			}

			assert.Len(s.T(), rows, 1)
			assert.NotNil(s.T(), md)
			return true
		}, 30*time.Second, 1*time.Second)
	})

	s.Run("InvalidQueryStatement", func() {
		client, err := queryClient.Query(context.Background(), &query_v1.QueryRequest{
			Statement: "FINAGLE * FROM default._default._default",
		}, grpc.PerRPCCredentials(s.badRpcCreds))
		assertRpcStatus(s.T(), err, codes.OK)

		_, _, err = readQueryStream(client)
		assertRpcStatus(s.T(), err, codes.InvalidArgument)
	})

	s.Run("BadCredentials", func() {
		client, err := queryClient.Query(context.Background(), &query_v1.QueryRequest{
			Statement: "SELECT * FROM default._default._default",
		}, grpc.PerRPCCredentials(s.badRpcCreds))
		assertRpcStatus(s.T(), err, codes.OK)

		_, _, err = readQueryStream(client)
		assertRpcStatus(s.T(), err, codes.PermissionDenied)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "user")
		})
	})

	s.Run("Unauthenticated", func() {
		client, err := queryClient.Query(context.Background(), &query_v1.QueryRequest{
			Statement: "SELECT * FROM default._default._default",
		})
		assertRpcStatus(s.T(), err, codes.OK)

		_, _, err = readQueryStream(client)
		assertRpcStatus(s.T(), err, codes.Unauthenticated)
	})
}
