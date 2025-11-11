package test

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/couchbase/stellar-gateway/utils/certificates"
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

	clientCert, err := certificates.GenerateSignedClientCert(s.caCert, s.caKey, username)
	assert.NoError(s.T(), err)

	conn, err := grpc.NewClient(s.gwConnAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs:            s.clientCaCertPool,
			Certificates:       []tls.Certificate{*clientCert},
			InsecureSkipVerify: false,
		})))
	if err != nil {
		s.T().Fatalf("failed to connect to test gateway: %s", err)
	}

	kvClient := kv_v1.NewKvServiceClient(conn)

	s.Run("UserMissing", func() {
		_, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
		})
		assertRpcStatus(s.T(), err, codes.PermissionDenied)
		assert.Contains(s.T(), err.Error(), "Your certificate is invalid")
	})

	// Create a user without any permissions
	dino.AddUser(username, false, false)

	s.T().Cleanup(func() {
		dino.RemoveUser(username)
	})

	s.Run("NoReadPermission", func() {
		_, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
		})
		assertRpcStatus(s.T(), err, codes.PermissionDenied)
		assert.Contains(s.T(), err.Error(), "No permissions to read documents")
	})

	// Add read permissions to the user and wait for this to take effect
	dino.AddUser(username, true, false)
	time.Sleep(time.Second * 5)

	s.Run("ReadSuccess", func() {
		resp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
		})
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), TEST_CONTENT, resp.GetContentUncompressed())
		assert.Nil(s.T(), resp.GetContentCompressed())
		assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)
		assert.Nil(s.T(), resp.Expiry)
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

	// Add write permissions to the user
	dino.AddUser(username, false, true)
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
