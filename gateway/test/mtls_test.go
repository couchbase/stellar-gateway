package test

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/goprotostellar/genproto/admin_bucket_v1"
	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"github.com/couchbase/goprotostellar/genproto/admin_query_v1"
	"github.com/couchbase/goprotostellar/genproto/admin_search_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/couchbase/goprotostellar/genproto/search_v1"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

func (s *GatewayOpsTestSuite) TestClientCertAuth() {
	testutils.SkipIfNoDinoCluster(s.T())

	indexClient := admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)
	indexName := "a" + uuid.NewString()[:6]
	sourceType := "couchbase"
	resp, err := indexClient.CreateIndex(context.Background(), &admin_search_v1.CreateIndexRequest{
		Name:       indexName,
		SourceName: &s.bucketName,
		SourceType: &sourceType,
		Type:       "fulltext-index",
		Params:     make(map[string][]byte),
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	type test struct {
		description string
		testFn      func(*grpc.ClientConn) (interface{}, error)
		errMsg      string
	}

	tests := []test{
		{
			description: "KvService",
			testFn: func(conn *grpc.ClientConn) (interface{}, error) {
				client := kv_v1.NewKvServiceClient(conn)
				return client.Get(context.Background(), &kv_v1.GetRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            s.testDocId(),
				})
			},
			errMsg: "No permissions to read documents",
		},
		{
			description: "QueryService",
			testFn: func(conn *grpc.ClientConn) (interface{}, error) {
				client := query_v1.NewQueryServiceClient(conn)
				docId := s.testDocId()
				bucketName := "default"
				resp, err := client.Query(context.Background(), &query_v1.QueryRequest{
					BucketName: &bucketName,
					Statement:  "SELECT * FROM default._default._default WHERE META().id='" + docId + "'",
				})
				requireRpcSuccess(s.T(), client, err)
				return resp.Recv()
			},
			errMsg: "No permissions to query documents.",
		},
		{
			description: "SearchService",
			testFn: func(conn *grpc.ClientConn) (interface{}, error) {
				client := search_v1.NewSearchServiceClient(conn)
				field := "service"
				query := &search_v1.Query_TermQuery{
					TermQuery: &search_v1.TermQuery{
						Term:  "search",
						Field: &field,
					},
				}
				resp, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
					IndexName: indexName,
					Query: &search_v1.Query{
						Query: query,
					},
				})
				requireRpcSuccess(s.T(), client, err)
				return resp.Recv()
			},
			errMsg: "No permissions to query documents.",
		},
		{
			description: "BucketMgmtService",
			testFn: func(conn *grpc.ClientConn) (interface{}, error) {
				client := admin_bucket_v1.NewBucketAdminServiceClient(conn)
				return client.ListBuckets(context.Background(), &admin_bucket_v1.ListBucketsRequest{})
			},
			errMsg: "No permissions to perform bucket management operation.",
		},
		{
			description: "CollectionMgmtService",
			testFn: func(conn *grpc.ClientConn) (interface{}, error) {
				client := admin_collection_v1.NewCollectionAdminServiceClient(conn)
				return client.ListCollections(context.Background(), &admin_collection_v1.ListCollectionsRequest{
					BucketName: s.bucketName,
				})
			},
			errMsg: "No permissions to perform collection management operation.",
		},
		{
			description: "QueryMgmtService",
			testFn: func(conn *grpc.ClientConn) (interface{}, error) {
				client := admin_query_v1.NewQueryAdminServiceClient(conn)
				indexName := uuid.NewString()
				return client.CreatePrimaryIndex(context.Background(), &admin_query_v1.CreatePrimaryIndexRequest{
					Name:           &indexName,
					BucketName:     s.bucketName,
					ScopeName:      &s.scopeName,
					CollectionName: &s.collectionName,
				})
			},
			errMsg: "Insufficient permissions to perform query index operation",
		},
		// BUG(ING-1368): unknown error returned when user lacks permissions
		// {
		// 	description: "SearchMgmtService",
		// 	testFn: func(conn *grpc.ClientConn) (interface{}, error) {
		// 		client := admin_search_v1.NewSearchAdminServiceClient(conn)
		// 		return client.DeleteIndex(context.Background(), &admin_search_v1.DeleteIndexRequest{Name: indexName})
		// 	},
		// 	errMsg: "No permissions to manage search indexes.",
		// },
	}

	for _, t := range tests {
		s.Run(t.description, func() {
			conn := s.connFromCert(s.missingUserCert)

			s.Run("UserMissing", func() {
				_, err := t.testFn(conn)
				assertRpcStatus(s.T(), err, codes.PermissionDenied)
				assert.Contains(s.T(), err.Error(), "Your certificate is invalid")
			})

			conn = s.connFromCert(s.noPermsCert)

			s.Run("NoUserPermissions", func() {
				_, err := t.testFn(conn)
				assertRpcStatus(s.T(), err, codes.PermissionDenied)
				assert.Contains(s.T(), err.Error(), t.errMsg)
			})

			conn = s.connFromCert(s.basicUserCert)

			s.Run("Success", func() {
				resp, err := t.testFn(conn)
				requireRpcSuccess(s.T(), resp, err)
			})
		})
	}
}

func (s *GatewayOpsTestSuite) TestClientCertAuthConfiguration() {
	testutils.SkipIfNoDinoCluster(s.T())
	conn := s.connFromCert(s.basicUserCert)
	kvClient := kv_v1.NewKvServiceClient(conn)

	getFn := func() (*kv_v1.GetResponse, error) {
		return kvClient.Get(context.Background(), &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            s.testDocId(),
		})
	}

	enableReq := &cbmgmtx.ConfigureClientCertAuthRequest{
		State: "enable",
		Prefixes: []cbmgmtx.Prefix{
			{
				Path:      "san.email",
				Prefix:    "",
				Delimiter: "@",
			},
		},
	}

	// Check that client cert auth is working as expected.
	s.Run("InitialSuccess", func() {
		resp, err := getFn()
		requireRpcSuccess(s.T(), resp, err)
	})

	ep, err := s.testClusterInfo.AdminClient.GetMgmtEndpoint(context.Background())
	require.NoError(s.T(), err)
	mgmt := cbmgmtx.Management{
		Transport: ep.RoundTripper,
		UserAgent: "useragent",
		Endpoint:  ep.Endpoint,
		Auth: &cbhttpx.BasicAuth{
			Username: ep.Username,
			Password: ep.Password,
		},
	}

	// Change the path that cbauth will try and get the name from and check
	// that the old cert fails
	err = mgmt.ConfigureClientCertAuth(context.Background(), &cbmgmtx.ConfigureClientCertAuthRequest{
		State: "enable",
		Prefixes: []cbmgmtx.Prefix{
			{
				Path:      "subject.cn",
				Prefix:    "",
				Delimiter: "",
			},
		},
	})
	assert.NoError(s.T(), err)

	s.Run("IncorrectUsernamePath", func() {
		require.Eventually(s.T(), func() bool {
			_, err := getFn()
			if err == nil {
				return false
			}

			assertRpcStatus(s.T(), err, codes.PermissionDenied)
			return assert.Contains(s.T(), err.Error(), "Your certificate is invalid")
		}, time.Second*30, time.Second*5)
	})

	// Restore intial settings and check that the original cert works again.
	err = mgmt.ConfigureClientCertAuth(context.Background(), enableReq)
	assert.NoError(s.T(), err)

	s.Run("SuccessAfterSettingsReset", func() {
		require.Eventually(s.T(), func() bool {
			resp, err := getFn()
			if err != nil {
				return false
			}

			requireRpcSuccess(s.T(), resp, err)
			return true
		}, time.Second*30, time.Second*5)
	})

	// Disable client cert auth on the cluster and make sure op fails.
	err = mgmt.ConfigureClientCertAuth(context.Background(), &cbmgmtx.ConfigureClientCertAuthRequest{
		State: "disable",
		Prefixes: []cbmgmtx.Prefix{
			{
				Path:      "san.email",
				Prefix:    "",
				Delimiter: "@",
			},
		},
	})
	assert.NoError(s.T(), err)

	s.Run("CertAuthDisabled", func() {
		require.Eventually(s.T(), func() bool {
			_, err := getFn()
			if err == nil {
				return false
			}

			assertRpcStatus(s.T(), err, codes.Unauthenticated)
			return assert.Contains(s.T(), err.Error(), "Client cert auth disabled on the cluster")
		}, time.Second*30, time.Second*5)
	})

	err = mgmt.ConfigureClientCertAuth(context.Background(), enableReq)
	assert.NoError(s.T(), err)

	// Ensure that client cert auth is full enabled for finishing the test so
	// that we don't impact other cert auth tests.
	require.Eventually(s.T(), func() bool {
		resp, err := getFn()
		if err != nil {
			return false
		}

		requireRpcSuccess(s.T(), resp, err)
		return true
	}, time.Second*30, time.Second*5)
}

func (s *GatewayOpsTestSuite) connFromCert(cert *tls.Certificate) *grpc.ClientConn {
	conn, err := grpc.NewClient(s.gwConnAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs:      s.clientCaCertPool,
			Certificates: []tls.Certificate{*cert},
		})))
	if err != nil {
		s.T().Fatalf("failed to connect to test gateway: %s", err)
	}

	return conn
}
