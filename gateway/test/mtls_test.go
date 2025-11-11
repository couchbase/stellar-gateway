package test

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

func (s *GatewayOpsTestSuite) TestClientCertAuth() {
	testutils.SkipIfNoDinoCluster(s.T())

	s.Run("KvService", s.KvService)
}

func (s *GatewayOpsTestSuite) KvService() {
	dino := testutils.StartDinoTesting(s.T(), false)
	username := "kvUser"
	conn := s.newClientCertConn(dino, username)
	kvClient := kv_v1.NewKvServiceClient(conn)
	getFn := func() (*kv_v1.GetResponse, error) {
		return kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
		})
	}

	s.Run("UserMissing", func() {
		_, err := getFn()
		assertRpcStatus(s.T(), err, codes.PermissionDenied)
		assert.Contains(s.T(), err.Error(), "Your certificate is invalid")
	})

	dino.AddUnprivilegedUser(username)
	time.Sleep(time.Second * 5)
	s.T().Cleanup(func() {
		dino.RemoveUser(username)
	})

	s.Run("NoUserPermissions", func() {
		_, err := getFn()
		assertRpcStatus(s.T(), err, codes.PermissionDenied)
		assert.Contains(s.T(), err.Error(), "No permissions to read documents")
	})

	dino.AddReadOnlyUser(username)
	time.Sleep(time.Second * 5)

	s.Run("ReadSuccess", func() {
		resp, err := getFn()
		requireRpcSuccess(s.T(), resp, err)
	})

	s.Run("NoWritePermission", func() {
		docId := s.randomDocId()
		_, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		})
		assertRpcStatus(s.T(), err, codes.PermissionDenied)
		assert.Contains(s.T(), err.Error(), "No permissions to write documents")
	})

	dino.AddWriteUser(username)
	time.Sleep(time.Second * 5)

	s.Run("WriteSuccess", func() {
		docId := s.randomDocId()
		resp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
		})
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assertValidMutationToken(s.T(), resp.MutationToken, s.bucketName)
	})
}

func (s *GatewayOpsTestSuite) newClientCertConn(dino *testutils.DinoController, username string) *grpc.ClientConn {
	res := dino.GetClientCert(username)

	cert, err := tls.X509KeyPair([]byte(res), []byte(res))
	assert.NoError(s.T(), err)

	conn, err := grpc.NewClient(s.gwConnAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs:      s.clientCaCertPool,
			Certificates: []tls.Certificate{cert},
		})))
	if err != nil {
		s.T().Fatalf("failed to connect to test gateway: %s", err)
	}

	return conn
}
