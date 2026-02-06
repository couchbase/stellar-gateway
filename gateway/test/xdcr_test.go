package test

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *GatewayOpsTestSuite) TestXdcrHeartbeat() {
	xdcrClient := internal_xdcr_v1.NewXdcrServiceClient(s.gatewayConn)

	_, err := xdcrClient.Heartbeat(context.Background(),
		&internal_xdcr_v1.HeartbeatRequest{
			Payload: []byte(`{"Magic":1,"ReqType":7,"Sender":"127.0.0.1:9000","TargetAddr":"127.0.0.1:18098","Opaque":4117233664,"LocalLifeCycleId":"","RemoteLifeCycleId":"","SourceClusterUUID":"fc6408545929b047b1ef357473117d76","SourceClusterName":"Source","NodesList":["127.0.0.1:9000"],"ProxyMode":true,"TTL":600000000000,"SendTime":"2025-10-24T19:31:37.764851+05:30","SpecsCompressed":"BAxudWxs"}`),
		},
		grpc.PerRPCCredentials(s.basicRpcCreds))

	if s.IsOlderServerVersion("8.0.0") {
		require.Error(s.T(), err)
	} else {
		require.NoError(s.T(), err)
	}
}

func (s *GatewayOpsTestSuite) TestXdcrGetClusterInfo() {
	xdcrClient := internal_xdcr_v1.NewXdcrServiceClient(s.gatewayConn)

	clusterInfoResp, err := xdcrClient.GetClusterInfo(context.Background(),
		&internal_xdcr_v1.GetClusterInfoRequest{},
		grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), clusterInfoResp, err)
	require.NotEmpty(s.T(), clusterInfoResp.ClusterUuid)
}

func (s *GatewayOpsTestSuite) TestXdcrGetBucketInfo() {
	xdcrClient := internal_xdcr_v1.NewXdcrServiceClient(s.gatewayConn)

	bucketInfoResp, err := xdcrClient.GetBucketInfo(context.Background(), &internal_xdcr_v1.GetBucketInfoRequest{
		BucketName: s.bucketName,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), bucketInfoResp, err)
	require.NotEmpty(s.T(), bucketInfoResp.BucketUuid)
	require.Greater(s.T(), bucketInfoResp.NumVbuckets, uint32(0))
	require.NotNil(s.T(), bucketInfoResp.BucketType)
	require.Equal(s.T(), internal_xdcr_v1.BucketType_BUCKET_TYPE_COUCHBASE, *bucketInfoResp.BucketType)
}

func (s *GatewayOpsTestSuite) TestXdcrGetVbucketInfo() {
	xdcrClient := internal_xdcr_v1.NewXdcrServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		bucketInfoResp, err := xdcrClient.GetBucketInfo(context.Background(), &internal_xdcr_v1.GetBucketInfoRequest{
			BucketName: s.bucketName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), bucketInfoResp, err)
		numVbuckets := bucketInfoResp.NumVbuckets

		// need to have at least 10 for this test
		require.Greater(s.T(), numVbuckets, uint32(10))

		client, err := xdcrClient.GetVbucketInfo(context.Background(), &internal_xdcr_v1.GetVbucketInfoRequest{
			BucketName:     s.bucketName,
			VbucketIds:     []uint32{0, 1, 2},
			IncludeHistory: ptr.To(true),
			IncludeMaxCas:  ptr.To(true),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), client, err)

		seenVbuckets := make(map[uint32]bool)
		for {
			resp, err := client.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				s.T().Fatalf("Failed to receive response: %v", err)
			}

			for _, vb := range resp.Vbuckets {
				if seenVbuckets[vb.VbucketId] {
					s.T().Fatalf("Received duplicate vbucket id: %d", vb.VbucketId)
				}
				seenVbuckets[vb.VbucketId] = true

				assert.Greater(s.T(), vb.HighSeqno, uint64(0))

				require.NotNil(s.T(), vb.History)
				assert.Greater(s.T(), len(vb.History), 0)
				for entryIdx, entry := range vb.History {
					assert.Greater(s.T(), entry.Uuid, uint64(0))
					if entryIdx < len(vb.History)-1 {
						// last entry seqno is always 0
						assert.Greater(s.T(), entry.Seqno, uint64(0))
					}
				}

				require.NotNil(s.T(), vb.MaxCas)
				assert.Greater(s.T(), *vb.MaxCas, uint64(0))
			}
		}

		for vbIdx := uint32(0); vbIdx < 3; vbIdx++ {
			if !seenVbuckets[vbIdx] {
				s.T().Fatalf("Did not receive vbucket id: %d", vbIdx)
			}
		}
	})

	s.Run("NoHistoryNoMaxCas", func() {
		bucketInfoResp, err := xdcrClient.GetBucketInfo(context.Background(), &internal_xdcr_v1.GetBucketInfoRequest{
			BucketName: s.bucketName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), bucketInfoResp, err)
		numVbuckets := bucketInfoResp.NumVbuckets

		// need to have at least 10 for this test
		require.Greater(s.T(), numVbuckets, uint32(10))

		client, err := xdcrClient.GetVbucketInfo(context.Background(), &internal_xdcr_v1.GetVbucketInfoRequest{
			BucketName: s.bucketName,
			VbucketIds: []uint32{0, 1, 2},
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), client, err)

		seenVbuckets := make(map[uint32]bool)
		for {
			resp, err := client.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				s.T().Fatalf("Failed to receive response: %v", err)
			}

			for _, vb := range resp.Vbuckets {
				if seenVbuckets[vb.VbucketId] {
					s.T().Fatalf("Received duplicate vbucket id: %d", vb.VbucketId)
				}
				seenVbuckets[vb.VbucketId] = true

				assert.Greater(s.T(), vb.HighSeqno, uint64(0))
				assert.Nil(s.T(), vb.History)
				assert.Nil(s.T(), vb.MaxCas)
			}
		}

		for vbIdx := uint32(0); vbIdx < 3; vbIdx++ {
			if !seenVbuckets[vbIdx] {
				s.T().Fatalf("Did not receive vbucket id: %d", vbIdx)
			}
		}
	})

	s.Run("AllVbuckets", func() {
		bucketInfoResp, err := xdcrClient.GetBucketInfo(context.Background(), &internal_xdcr_v1.GetBucketInfoRequest{
			BucketName: s.bucketName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), bucketInfoResp, err)
		numVbuckets := bucketInfoResp.NumVbuckets

		client, err := xdcrClient.GetVbucketInfo(context.Background(), &internal_xdcr_v1.GetVbucketInfoRequest{
			BucketName: s.bucketName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), client, err)

		seenVbuckets := make(map[uint32]bool)
		for {
			resp, err := client.Recv()
			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				s.T().Fatalf("Failed to receive response: %v", err)
			}

			for _, vb := range resp.Vbuckets {
				if seenVbuckets[vb.VbucketId] {
					s.T().Fatalf("Received duplicate vbucket id: %d", vb.VbucketId)
				}
				seenVbuckets[vb.VbucketId] = true

				assert.Greater(s.T(), vb.HighSeqno, uint64(0))
				assert.Nil(s.T(), vb.History)
				assert.Nil(s.T(), vb.MaxCas)
			}
		}

		for vbIdx := uint32(0); vbIdx < numVbuckets; vbIdx++ {
			if !seenVbuckets[vbIdx] {
				s.T().Fatalf("Did not receive vbucket id: %d", vbIdx)
			}
		}
	})
}

func (s *GatewayOpsTestSuite) TestXdcrWatchCollections() {
	xdcrClient := internal_xdcr_v1.NewXdcrServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		opCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		resp, err := xdcrClient.WatchCollections(opCtx, &internal_xdcr_v1.WatchCollectionsRequest{
			BucketName: s.bucketName,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)

		manifest, err := resp.Recv()
		require.NoError(s.T(), err)
		require.Greater(s.T(), manifest.ManifestUid, uint32(0))
		require.Greater(s.T(), len(manifest.Scopes), 0)

		for _, scope := range manifest.Scopes {
			if scope.ScopeName == "_default" {
				require.Zero(s.T(), scope.ScopeId)
			} else {
				require.Greater(s.T(), scope.ScopeId, uint32(0))
			}
			require.NotEmpty(s.T(), scope.ScopeName)
			require.Greater(s.T(), len(scope.Collections), 0)

			for _, collection := range scope.Collections {
				if collection.CollectionName == "_default" {
					require.Zero(s.T(), collection.CollectionId)
				} else {
					require.Greater(s.T(), collection.CollectionId, uint32(0))
				}
				require.NotEmpty(s.T(), collection.CollectionName)
			}
		}
	})

	s.Run("BucketNotFound", func() {
		opCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		resp, err := xdcrClient.WatchCollections(opCtx, &internal_xdcr_v1.WatchCollectionsRequest{
			BucketName: "nonexistent-bucket",
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)

		_, err = resp.Recv()
		assertRpcStatus(s.T(), err, codes.NotFound)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), "bucket", d.ResourceType)
		})
	})
}

func (s *GatewayOpsTestSuite) TestXdcrGetDocument() {
	xdcrClient := internal_xdcr_v1.NewXdcrServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
		docId := s.testDocId()

		resp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			IncludeContent: false,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), []byte(nil), resp.ContentCompressed)
		assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)
		assert.Nil(s.T(), resp.Expiry)
		assert.Len(s.T(), resp.Xattrs, 0)
	})

	s.Run("WithMeta", func() {
		docId := s.testDocId()

		resp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			IncludeContent: true,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), resp, err)
		assertValidCas(s.T(), resp.Cas)
		assert.Equal(s.T(), TEST_CONTENT, s.decompressContent(resp.ContentCompressed))
		assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)
		assert.Nil(s.T(), resp.Expiry)
		assert.Len(s.T(), resp.Xattrs, 0)
	})

	s.Run("WithXattrs", func() {
		docId := s.testDocIdWithXattrs()

		resp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			IncludeContent: true,
			IncludeXattrs:  ptr.To(true),
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		if s.IsOlderServerVersion("8.0.0") {
			assertRpcStatus(s.T(), err, codes.Unimplemented)
		} else {
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)
			assert.Equal(s.T(), TEST_CONTENT, s.decompressContent(resp.ContentCompressed))
			assert.Equal(s.T(), TEST_CONTENT_FLAGS, resp.ContentFlags)
			assert.Nil(s.T(), resp.Expiry)
			assert.Len(s.T(), resp.Xattrs, 1)
			assert.Equal(s.T(), []byte(`{"hello":"world"}`), resp.Xattrs["test"])
		}
	})
}

func (s *GatewayOpsTestSuite) TestXdcrCheckDocument() {
	xdcrClient := internal_xdcr_v1.NewXdcrServiceClient(s.gatewayConn)

	s.Run("Add", func() {
		s.Run("Basic", func() {
			docId := s.randomDocId()

			// we just make up a cas for testing purposes
			var docCreateCas uint64 = 1234

			resp, err := xdcrClient.CheckDocument(context.Background(), &internal_xdcr_v1.CheckDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				StoreCas:       docCreateCas,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ExpiryTime:     nil, // no expiry
				Revno:          1,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
		})
	})

	s.Run("Set", func() {
		s.Run("Basic", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			setResp, err := xdcrClient.CheckDocument(context.Background(), &internal_xdcr_v1.CheckDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				StoreCas:       getResp.Cas + 10,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ExpiryTime:     nil, // no expiry
				Revno:          getResp.Revno + 10,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), setResp, err)
		})

		s.Run("LwwFail", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			_, err = xdcrClient.CheckDocument(context.Background(), &internal_xdcr_v1.CheckDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				StoreCas:       getResp.Cas - 1,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ExpiryTime:     nil, // no expiry
				Revno:          getResp.Revno - 1,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.Aborted)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
				assert.Equal(s.T(), "DOC_NEWER", d.Reason)
			})
		})

		s.Run("Xattr", func() {
			s.Run("Both", func() {
				docId := s.randomDocId()
				s.createDocument(createDocumentOptions{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					DocId:          docId,
					Content:        TEST_CONTENT,
					ContentFlags:   TEST_CONTENT_FLAGS,
					WithXattrs:     true,
				})

				getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), getResp, err)

				_, err = xdcrClient.CheckDocument(context.Background(), &internal_xdcr_v1.CheckDocumentRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					StoreCas:       getResp.Cas,
					ContentFlags:   TEST_CONTENT_FLAGS,
					ExpiryTime:     nil, // no expiry
					Revno:          getResp.Revno,
					HasXattrs:      true,
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), getResp, err)
			})

			s.Run("Neither", func() {
				docId := s.randomDocId()
				s.createDocument(createDocumentOptions{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					DocId:          docId,
					Content:        TEST_CONTENT,
					ContentFlags:   TEST_CONTENT_FLAGS,
				})

				getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), getResp, err)

				_, err = xdcrClient.CheckDocument(context.Background(), &internal_xdcr_v1.CheckDocumentRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					StoreCas:       getResp.Cas,
					ContentFlags:   TEST_CONTENT_FLAGS,
					ExpiryTime:     nil, // no expiry
					Revno:          getResp.Revno,
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), getResp, err)
			})

			s.Run("OursOnly", func() {
				docId := s.randomDocId()
				s.createDocument(createDocumentOptions{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					DocId:          docId,
					Content:        TEST_CONTENT,
					ContentFlags:   TEST_CONTENT_FLAGS,
					WithXattrs:     false,
				})

				getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), getResp, err)

				_, err = xdcrClient.CheckDocument(context.Background(), &internal_xdcr_v1.CheckDocumentRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					StoreCas:       getResp.Cas,
					ContentFlags:   TEST_CONTENT_FLAGS,
					ExpiryTime:     nil, // no expiry
					Revno:          getResp.Revno,
					HasXattrs:      true,
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), getResp, err)
			})

			s.Run("TheirsOnly", func() {
				docId := s.randomDocId()
				s.createDocument(createDocumentOptions{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					DocId:          docId,
					Content:        TEST_CONTENT,
					ContentFlags:   TEST_CONTENT_FLAGS,
					WithXattrs:     true,
				})

				getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				requireRpcSuccess(s.T(), getResp, err)

				_, err = xdcrClient.CheckDocument(context.Background(), &internal_xdcr_v1.CheckDocumentRequest{
					BucketName:     s.bucketName,
					ScopeName:      s.scopeName,
					CollectionName: s.collectionName,
					Key:            docId,
					StoreCas:       getResp.Cas,
					ContentFlags:   TEST_CONTENT_FLAGS,
					ExpiryTime:     nil, // no expiry
					Revno:          getResp.Revno,
					HasXattrs:      false,
				}, grpc.PerRPCCredentials(s.basicRpcCreds))
				assertRpcStatus(s.T(), err, codes.Aborted)
				assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
					assert.Equal(s.T(), "DOC_NEWER", d.Reason)
				})
			})
		})
	})

	s.Run("Delete", func() {
		s.Run("Basic", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				IncludeContent: false,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			delResp, err := xdcrClient.CheckDocument(context.Background(), &internal_xdcr_v1.CheckDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				StoreCas:       getResp.Cas + 10,
				Revno:          getResp.Revno + 10,
				IsDeleted:      true,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), delResp, err)
		})

		s.Run("LwwFail", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				IncludeContent: false,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			_, err = xdcrClient.CheckDocument(context.Background(), &internal_xdcr_v1.CheckDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				StoreCas:       getResp.Cas - 1,
				Revno:          getResp.Revno - 1,
				IsDeleted:      true,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.Aborted)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
				assert.Equal(s.T(), "DOC_NEWER", d.Reason)
			})
		})
	})
}

func (s *GatewayOpsTestSuite) TestXdcrPushDocument() {
	xdcrClient := internal_xdcr_v1.NewXdcrServiceClient(s.gatewayConn)

	s.Run("Add", func() {
		s.Run("Basic", func() {
			docId := s.randomDocId()

			// we pass a CAS of 0 to indicate that we want to create the document
			var docCheckCas uint64 = 0

			// we just make up a cas for testing purposes
			var docCreateCas uint64 = 1234

			resp, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &docCheckCas,
				StoreCas:       docCreateCas,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ContentType:    internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				Content: &internal_xdcr_v1.PushDocumentRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ExpiryTime: nil, // no expiry
				Revno:      1,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Cas:            docCreateCas,
			})
		})

		s.Run("WithXattrs", func() {
			if s.IsOlderServerVersion("8.0.0") {
				s.T().Skip("Cannot validate xattrs on server versions older than 8.0.0")
			}

			docId := s.randomDocId()

			// we pass a CAS of 0 to indicate that we want to create the document
			var docCheckCas uint64 = 0

			// we just make up a cas for testing purposes
			var docCreateCas uint64 = 1234

			resp, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &docCheckCas,
				StoreCas:       docCreateCas,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ContentType:    internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				Content: &internal_xdcr_v1.PushDocumentRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ExpiryTime: nil, // no expiry
				Revno:      1,
				Xattrs: map[string][]byte{
					"test": []byte(`{"hello":"world"}`),
				},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Cas:            docCreateCas,
			})

			xattrGetResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				IncludeContent: true,
				IncludeXattrs:  ptr.To(true),
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), xattrGetResp, err)
			assert.Equal(s.T(), map[string][]byte{
				"test": []byte(`{"hello":"world"}`),
			}, xattrGetResp.Xattrs)
		})

		s.Run("Compressed", func() {
			docId := s.randomDocId()

			// we pass a CAS of 0 to indicate that we want to create the document
			var docCheckCas uint64 = 0

			// we just make up a cas for testing purposes
			var docCreateCas uint64 = 1234

			resp, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &docCheckCas,
				StoreCas:       docCreateCas,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ContentType:    internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				Content: &internal_xdcr_v1.PushDocumentRequest_ContentCompressed{
					ContentCompressed: s.compressContent(TEST_CONTENT),
				},
				ExpiryTime: nil, // no expiry
				Revno:      1,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), resp, err)
			assertValidCas(s.T(), resp.Cas)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Cas:            docCreateCas,
			})
		})

		s.Run("VbUuidMismatch", func() {
			docId := s.randomDocId()

			// we pass a CAS of 0 to indicate that we want to create the document
			var docCheckCas uint64 = 0

			// we just make up a cas for testing purposes
			var docCreateCas uint64 = 1234

			// we just make up a vbuuid that won't match
			var vbuuidMismatch uint64 = 99999

			_, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &docCheckCas,
				StoreCas:       docCreateCas,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ContentType:    internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				Content: &internal_xdcr_v1.PushDocumentRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ExpiryTime: nil, // no expiry
				Revno:      1,
				VbUuid:     &vbuuidMismatch,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.Aborted)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
				assert.Equal(s.T(), "VBUUID_MISMATCH", d.Reason)
			})
		})

		s.Run("PoisonedCas", func() {
			docId := s.randomDocId()

			// we pass a CAS of 0 to indicate that we want to create the document
			var docCheckCas uint64 = 0

			var poisonedCas = uint64(time.Now().Add(2 * time.Hour).UnixNano())

			resp, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &docCheckCas,
				StoreCas:       poisonedCas,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ContentType:    internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				Content: &internal_xdcr_v1.PushDocumentRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ExpiryTime: nil, // no expiry
				Revno:      1,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))

			// Prior to 8.0 there is no validation performed by the server on
			// the store cas
			if s.IsOlderServerVersion("8.0.0") {
				requireRpcSuccess(s.T(), resp, err)
				return
			}

			assertRpcStatus(s.T(), err, codes.FailedPrecondition)
			assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
				assert.Len(s.T(), d.Violations, 1)
				assert.Equal(s.T(), "POISONED_CAS", d.Violations[0].Type)
			})
		})
	})

	s.Run("Set", func() {
		s.Run("Basic", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			setResp, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &getResp.Cas,
				StoreCas:       getResp.Cas + 1,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ContentType:    internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				Content: &internal_xdcr_v1.PushDocumentRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ExpiryTime: nil, // no expiry
				Revno:      getResp.Revno + 1,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), setResp, err)
			assertValidCas(s.T(), setResp.Cas)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Cas:            getResp.Cas + 1,
			})
		})

		s.Run("WithXattrs", func() {
			if s.IsOlderServerVersion("8.0.0") {
				s.T().Skip("Cannot validate xattrs on server versions older than 8.0.0")
			}

			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			setResp, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &getResp.Cas,
				StoreCas:       getResp.Cas + 1,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ContentType:    internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				Content: &internal_xdcr_v1.PushDocumentRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ExpiryTime: nil, // no expiry
				Revno:      getResp.Revno + 1,
				Xattrs: map[string][]byte{
					"test": []byte(`{"hello":"world"}`),
				},
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), setResp, err)
			assertValidCas(s.T(), setResp.Cas)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Cas:            getResp.Cas + 1,
			})

			xattrGetResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				IncludeContent: true,
				IncludeXattrs:  ptr.To(true),
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), xattrGetResp, err)
			assert.Equal(s.T(), map[string][]byte{
				"test": []byte(`{"hello":"world"}`),
			}, xattrGetResp.Xattrs)
		})

		s.Run("Compressed", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			setResp, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &getResp.Cas,
				StoreCas:       getResp.Cas + 1,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ContentType:    internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				Content: &internal_xdcr_v1.PushDocumentRequest_ContentCompressed{
					ContentCompressed: s.compressContent(TEST_CONTENT),
				},
				ExpiryTime: nil, // no expiry
				Revno:      getResp.Revno + 1,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), setResp, err)
			assertValidCas(s.T(), setResp.Cas)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Cas:            getResp.Cas + 1,
			})
		})

		s.Run("CasMismatch", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			// intentionally pick a CAS that wont match
			var wrongCas = getResp.Cas + 1

			_, err = xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &wrongCas,
				StoreCas:       getResp.Cas + 1,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ContentType:    internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				Content: &internal_xdcr_v1.PushDocumentRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ExpiryTime: nil, // no expiry
				Revno:      getResp.Revno + 1,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.Aborted)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
				assert.Equal(s.T(), "CAS_MISMATCH", d.Reason)
			})
		})

		s.Run("BasicLww", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			setResp, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       nil,
				StoreCas:       getResp.Cas + 10,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ContentType:    internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				Content: &internal_xdcr_v1.PushDocumentRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ExpiryTime: nil, // no expiry
				Revno:      getResp.Revno + 10,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), setResp, err)
			assertValidCas(s.T(), setResp.Cas)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				Cas:            getResp.Cas + 10,
			})
		})

		s.Run("LwwFail", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			_, err = xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       nil,
				StoreCas:       getResp.Cas - 1,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ContentType:    internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				Content: &internal_xdcr_v1.PushDocumentRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ExpiryTime: nil, // no expiry
				Revno:      getResp.Revno - 1,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.Aborted)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
				assert.Equal(s.T(), "DOC_NEWER", d.Reason)
			})
		})

		s.Run("VbUuidMismatch", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			// we just make up a vbuuid that won't match
			var vbuuidMismatch uint64 = 99999

			_, err = xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &getResp.Cas,
				StoreCas:       getResp.Cas + 1,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ContentType:    internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				Content: &internal_xdcr_v1.PushDocumentRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ExpiryTime: nil, // no expiry
				Revno:      getResp.Revno + 1,
				VbUuid:     &vbuuidMismatch,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.Aborted)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
				assert.Equal(s.T(), "VBUUID_MISMATCH", d.Reason)
			})
		})

		s.Run("PoisonedCas", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			var poisonedCas = uint64(time.Now().Add(2 * time.Hour).UnixNano())

			resp, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &getResp.Cas,
				StoreCas:       poisonedCas,
				ContentFlags:   TEST_CONTENT_FLAGS,
				ContentType:    internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				Content: &internal_xdcr_v1.PushDocumentRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ExpiryTime: nil, // no expiry
				Revno:      getResp.Revno + 1,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))

			// Prior to 8.0 there is no validation performed by the server on
			// the store cas
			if s.IsOlderServerVersion("8.0.0") {
				requireRpcSuccess(s.T(), resp, err)
				return
			}

			assertRpcStatus(s.T(), err, codes.FailedPrecondition)
			assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
				assert.Len(s.T(), d.Violations, 1)
				assert.Equal(s.T(), "POISONED_CAS", d.Violations[0].Type)
			})
		})
	})

	s.Run("Delete", func() {
		s.Run("Basic", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				IncludeContent: false,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			delResp, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &getResp.Cas,
				StoreCas:       getResp.Cas + 1,
				IsDeleted:      true,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), delResp, err)
		})

		s.Run("BasicLww", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				IncludeContent: false,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			delResp, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       nil,
				StoreCas:       getResp.Cas + 10,
				Revno:          getResp.Revno + 10,
				IsDeleted:      true,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), delResp, err)
		})

		s.Run("LwwFail", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				IncludeContent: false,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			_, err = xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       nil,
				StoreCas:       getResp.Cas - 1,
				Revno:          getResp.Revno - 1,
				IsDeleted:      true,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.Aborted)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
				assert.Equal(s.T(), "DOC_NEWER", d.Reason)
			})
		})

		s.Run("VbUuidMismatch", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				IncludeContent: false,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			var vbuuidMismatch uint64 = 99999

			_, err = xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &getResp.Cas,
				StoreCas:       getResp.Cas + 1,
				IsDeleted:      true,
				VbUuid:         &vbuuidMismatch,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.Aborted)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
				assert.Equal(s.T(), "VBUUID_MISMATCH", d.Reason)
			})
		})

		s.Run("PoisonedCas", func() {
			docId := s.testDocId()

			getResp, err := xdcrClient.GetDocument(context.Background(), &internal_xdcr_v1.GetDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				IncludeContent: false,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), getResp, err)

			var poisonedCas = uint64(time.Now().Add(2 * time.Hour).UnixNano())

			resp, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				CheckCas:       &getResp.Cas,
				StoreCas:       poisonedCas,
				IsDeleted:      true,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))

			// Prior to 8.0 there is no validation performed by the server on
			// the store cas
			if s.IsOlderServerVersion("8.0.0") {
				requireRpcSuccess(s.T(), resp, err)
				return
			}

			assertRpcStatus(s.T(), err, codes.FailedPrecondition)
			assertRpcErrorDetails(s.T(), err, func(d *epb.PreconditionFailure) {
				assert.Len(s.T(), d.Violations, 1)
				assert.Equal(s.T(), "POISONED_CAS", d.Violations[0].Type)
			})
		})
	})
}
