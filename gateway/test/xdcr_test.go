package test

import (
	"context"
	"errors"
	"io"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/stretchr/testify/assert"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *GatewayOpsTestSuite) TestXdcrGetVbucketInfo() {
	xdcrClient := internal_xdcr_v1.NewXdcrServiceClient(s.gatewayConn)

	s.Run("Basic", func() {
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

				assert.Greater(s.T(), len(vb.FailoverLog), 0)
				for entryIdx, entry := range vb.FailoverLog {
					assert.Greater(s.T(), entry.Uuid, uint64(0))
					if entryIdx < len(vb.FailoverLog)-1 {
						// last entry seqno is always 0
						assert.Greater(s.T(), entry.Seqno, uint64(0))
					}
				}
				assert.Greater(s.T(), vb.HighSeqno, uint64(0))
				assert.Greater(s.T(), vb.MaxCas, uint64(0))
			}
		}

		numVbuckets := uint32(1024)
		for vbIdx := uint32(0); vbIdx < numVbuckets; vbIdx++ {
			if !seenVbuckets[vbIdx] {
				s.T().Fatalf("Did not receive vbucket id: %d", vbIdx)
			}
		}
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
				BucketName:        s.bucketName,
				ScopeName:         s.scopeName,
				CollectionName:    s.collectionName,
				Key:               docId,
				CheckCas:          &docCheckCas,
				StoreCas:          docCreateCas,
				ContentFlags:      TEST_CONTENT_FLAGS,
				ContentType:       internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				ContentCompressed: s.compressContent(TEST_CONTENT),
				ExpiryTime:        nil, // no expiry
				Revno:             1,
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

		s.Run("DocExists", func() {
			docId := s.testDocId()

			// we pass a CAS of 0 to indicate that we want to create the document
			var docCheckCas uint64 = 0

			_, err := xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:        s.bucketName,
				ScopeName:         s.scopeName,
				CollectionName:    s.collectionName,
				Key:               docId,
				CheckCas:          &docCheckCas,
				StoreCas:          1234,
				ContentFlags:      TEST_CONTENT_FLAGS,
				ContentType:       internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				ContentCompressed: s.compressContent(TEST_CONTENT),
				ExpiryTime:        nil, // no expiry
				Revno:             1,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.AlreadyExists)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
				assert.Equal(s.T(), "document", d.ResourceType)
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
				BucketName:        s.bucketName,
				ScopeName:         s.scopeName,
				CollectionName:    s.collectionName,
				Key:               docId,
				CheckCas:          &getResp.Cas,
				StoreCas:          getResp.Cas - 1,
				ContentFlags:      TEST_CONTENT_FLAGS,
				ContentType:       internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				ContentCompressed: s.compressContent(TEST_CONTENT),
				ExpiryTime:        nil, // no expiry
				Revno:             getResp.Revno + 1,
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
				Cas:            getResp.Cas - 1,
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
			var wrongCas uint64 = getResp.Cas + 1

			_, err = xdcrClient.PushDocument(context.Background(), &internal_xdcr_v1.PushDocumentRequest{
				BucketName:        s.bucketName,
				ScopeName:         s.scopeName,
				CollectionName:    s.collectionName,
				Key:               docId,
				CheckCas:          &wrongCas,
				StoreCas:          getResp.Cas + 1,
				ContentFlags:      TEST_CONTENT_FLAGS,
				ContentType:       internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				ContentCompressed: s.compressContent(TEST_CONTENT),
				ExpiryTime:        nil, // no expiry
				Revno:             getResp.Revno + 1,
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
				BucketName:        s.bucketName,
				ScopeName:         s.scopeName,
				CollectionName:    s.collectionName,
				Key:               docId,
				CheckCas:          nil,
				StoreCas:          getResp.Cas + 10,
				ContentFlags:      TEST_CONTENT_FLAGS,
				ContentType:       internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				ContentCompressed: s.compressContent(TEST_CONTENT),
				ExpiryTime:        nil, // no expiry
				Revno:             getResp.Revno + 10,
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
				BucketName:        s.bucketName,
				ScopeName:         s.scopeName,
				CollectionName:    s.collectionName,
				Key:               docId,
				CheckCas:          nil,
				StoreCas:          getResp.Cas - 1,
				ContentFlags:      TEST_CONTENT_FLAGS,
				ContentType:       internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON,
				ContentCompressed: s.compressContent(TEST_CONTENT),
				ExpiryTime:        nil, // no expiry
				Revno:             getResp.Revno - 1,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			assertRpcStatus(s.T(), err, codes.Aborted)
			assertRpcErrorDetails(s.T(), err, func(d *epb.ErrorInfo) {
				assert.Equal(s.T(), "DOC_NEWER", d.Reason)
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
	})
}
