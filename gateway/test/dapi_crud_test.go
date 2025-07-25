package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/golang/snappy"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type commonDapiTestData struct {
	ScopeName      string
	BucketName     string
	CollectionName string
	DocumentKey    string
	Headers        map[string]string
	Body           *[]byte
}

func (s *GatewayOpsTestSuite) RunCommonDapiErrorCases(
	fn func(opts *commonDapiTestData) *testHttpResponse,
) {
	s.Run("CollectionMissing", func() {
		resp := fn(&commonDapiTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: "invalid-collection",
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			DocumentKey: s.randomDocId(),
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "CollectionNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s",
				s.bucketName, s.scopeName, "invalid-collection"),
		})
	})

	s.Run("ScopeMissing", func() {
		resp := fn(&commonDapiTestData{
			BucketName:     s.bucketName,
			ScopeName:      "invalid-scope",
			CollectionName: s.collectionName,
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			DocumentKey: s.randomDocId(),
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "ScopeNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s",
				s.bucketName, "invalid-scope"),
		})
	})

	s.Run("BucketMissing", func() {
		resp := fn(&commonDapiTestData{
			BucketName:     "invalid-bucket",
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			DocumentKey: s.randomDocId(),
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code:     "BucketNotFound",
			Resource: fmt.Sprintf("/buckets/%s", "invalid-bucket"),
		})
	})

	s.Run("BadCredentials", func() {
		resp := fn(&commonDapiTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Headers: map[string]string{
				"Authorization": s.badRestCreds,
			},
			DocumentKey: s.randomDocId(),
		})
		requireRestError(s.T(), resp, http.StatusForbidden, &testRestError{
			Code: "InvalidAuth",
		})
	})

	s.Run("Unauthenticated", func() {
		resp := fn(&commonDapiTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Headers: map[string]string{
				"Lol": "14",
			},
			DocumentKey: s.randomDocId(),
		})
		require.NotNil(s.T(), resp)
		require.Equal(s.T(), http.StatusBadRequest, resp.StatusCode)
		// Authorization header missing is considered a missing parameter rather
		// than an authentication specific error.
	})

	s.Run("DocKeyTooLong", func() {
		resp := fn(&commonDapiTestData{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			DocumentKey: s.docIdOfLen(251),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})
}

func (s *GatewayOpsTestSuite) IterDapiDurabilityLevelTests(
	fn func(durabilityLevel string,
		assertFailure func(*testHttpResponse)),
) {
	DurabilityLevels := []string{"None", "Majority", "MajorityAndPersistOnMaster", "PersistToMajority"}
	for _, durabilityLevel := range DurabilityLevels {
		s.Run(fmt.Sprintf("DurabilityLevel%s", durabilityLevel), func() {
			fn(durabilityLevel, nil)
		})
	}

	s.Run("InvalidDurabilityLevel", func() {
		fn("invalid-level", func(resp *testHttpResponse) {
			requireRestError(s.T(), resp, http.StatusBadRequest, nil)
		})
	})

	s.Run("BlankDurabilityLevel", func() {
		fn("", func(resp *testHttpResponse) {
			requireRestError(s.T(), resp, http.StatusBadRequest, nil)
		})
	})
}

func (s *GatewayOpsTestSuite) IterDapiAcceptEncodingTests(
	fn func(acceptEncoding string,
		assertContentEncoding func(string),
		assertFailure func(*testHttpResponse)),
) {
	requireIdentityEncoding := func(contentEncoding string) {
		require.Equal(s.T(), "", contentEncoding)
	}
	requireSnappyEncoding := func(contentEncoding string) {
		require.Equal(s.T(), "snappy", contentEncoding)
	}
	requireSnappyOrIdentityEncoding := func(contentEncoding string) {
		if contentEncoding != "snappy" && contentEncoding != "" {
			require.Fail(s.T(), "Expected content encoding to be either 'snappy' or empty, got: %s", contentEncoding)
		}
	}

	s.Run("AcceptEncodingIdentity", func() {
		fn("identity", requireIdentityEncoding, nil)
	})

	s.Run("AcceptEncodingSnappy", func() {
		fn("snappy", requireSnappyOrIdentityEncoding, nil)
	})

	s.Run("AcceptEncodingUnknown", func() {
		fn("gzip;q=1.0,*;q=0.5", requireIdentityEncoding, nil)
	})

	s.Run("AcceptEncodingSnappyNoIdentity", func() {
		fn("snappy, identity;q=0", requireSnappyEncoding, nil)
	})

	s.Run("AcceptEncodingSnappyOnly", func() {
		fn("snappy, *;q=0", requireSnappyEncoding, nil)
	})

	s.Run("InvalidDocumentEncoding", func() {
		fn("inv-ali;;;2", requireIdentityEncoding, func(resp *testHttpResponse) {
			requireRestError(s.T(), resp, http.StatusBadRequest, nil)
		})
	})
}

func (s *GatewayOpsTestSuite) IterDapiDocumentEncodingTests(
	testContent []byte,
	fn func(contentEncoding string,
		encodedContent []byte,
		assertFailure func(*testHttpResponse)),
) {
	snappyContent := snappy.Encode(nil, testContent)

	s.Run("DocumentEncodingIdentity", func() {
		fn("identity", testContent, nil)
	})

	s.Run("DocumentEncodingSnappy", func() {
		fn("snappy", snappyContent, nil)
	})

	s.Run("InvalidDocumentEncoding", func() {
		fn("invalid", testContent, func(resp *testHttpResponse) {
			requireRestError(s.T(), resp, http.StatusBadRequest, nil)
		})
	})

	s.Run("BlankDocumentEncoding", func() {
		fn("", testContent, func(resp *testHttpResponse) {
			requireRestError(s.T(), resp, http.StatusBadRequest, nil)
		})
	})

	s.Run("DocumentEncodingSnappy_NotSnappyData", func() {
		// This is a test for the case where the content is not actually snappy encoded,
		// but the request says it is.
		fn("snappy", testContent, func(resp *testHttpResponse) {
			requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
				Code: "InvalidArgument",
			})
		})
	})
}

func (s *GatewayOpsTestSuite) TestDapiGet() {
	s.Run("Basic", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.Equal(s.T(), fmt.Sprintf("%d", TEST_CONTENT_FLAGS), resp.Headers.Get("X-CB-Flags"))
		assert.Equal(s.T(), "", resp.Headers.Get("Content-Encoding"))
		assert.Equal(s.T(), "application/json", resp.Headers.Get("Content-Type"))
		assert.Equal(s.T(), TEST_CONTENT, resp.Body)
		assert.Equal(s.T(), "", resp.Headers.Get("Expires"))
	})

	s.Run("NonBinary", func() {
		docBytes := []byte(`hello world`)
		docId := s.binaryDocId(docBytes)
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.Equal(s.T(), "0", resp.Headers.Get("X-CB-Flags"))
		assert.Equal(s.T(), "", resp.Headers.Get("Content-Encoding"))
		assert.Equal(s.T(), "text/plain", resp.Headers.Get("Content-Type"))
		assert.Equal(s.T(), docBytes, resp.Body)
		assert.Equal(s.T(), "", resp.Headers.Get("Expires"))
	})

	s.Run("OptionallyCompressed", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
			Headers: map[string]string{
				"Authorization":   s.basicRestCreds,
				"Accept-Encoding": "snappy",
			},
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.Equal(s.T(), fmt.Sprintf("%d", TEST_CONTENT_FLAGS), resp.Headers.Get("X-CB-Flags"))
		assert.Equal(s.T(), "", resp.Headers.Get("Content-Encoding"))
		assert.Equal(s.T(), TEST_CONTENT, resp.Body)
		assert.Equal(s.T(), "", resp.Headers.Get("Expires"))
	})

	s.Run("ForceCompressed", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
			Headers: map[string]string{
				"Authorization":   s.basicRestCreds,
				"Accept-Encoding": "snappy,identity;q=0",
			},
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.Equal(s.T(), fmt.Sprintf("%d", TEST_CONTENT_FLAGS), resp.Headers.Get("X-CB-Flags"))
		assert.Equal(s.T(), "snappy", resp.Headers.Get("Content-Encoding"))
		assert.Equal(s.T(), TEST_CONTENT, s.decompressContent(resp.Body))
		assert.Equal(s.T(), "", resp.Headers.Get("Expires"))
	})

	s.Run("ProjectSimple", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				`/v1/buckets/%s/scopes/%s/collections/%s/documents/%s?project=obj.num,arr`,
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.Equal(s.T(), fmt.Sprintf("%d", TEST_CONTENT_FLAGS), resp.Headers.Get("X-CB-Flags"))
		assert.JSONEq(s.T(), `{"obj":{"num":14},"arr":[3,6,9,12]}`, string(resp.Body))
		assert.Equal(s.T(), "", resp.Headers.Get("Expires"))
	})

	s.Run("ProjectNestedMissing", func() {
		docId := s.testDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				`/v1/buckets/%s/scopes/%s/collections/%s/documents/%s?project=arr,obj.num.nest`,
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "PathMismatch",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s/content/{obj.num.nest}",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("DocLocked", func() {
		docId := s.lockedDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				`/v1/buckets/%s/scopes/%s/collections/%s/documents/%s`,
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "DocumentLocked",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("DocMissing", func() {
		docId := s.missingDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				`/v1/buckets/%s/scopes/%s/collections/%s/documents/%s`,
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("DocKeyMaxLen", func() {
		docId := s.docIdOfLen(250)
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				`/v1/buckets/%s/scopes/%s/collections/%s/documents/%s`,
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("DocKeyMinLen", func() {
		docId := s.docIdOfLen(1)
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				`/v1/buckets/%s/scopes/%s/collections/%s/documents/%s`,
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.IterDapiAcceptEncodingTests(func(
		acceptEncoding string,
		assertContentEncoding func(string),
		assertFailure func(*testHttpResponse),
	) {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
			Headers: map[string]string{
				"Authorization":   s.basicRestCreds,
				"Accept-Encoding": acceptEncoding,
			},
		})
		if assertFailure != nil {
			assertFailure(resp)
			return
		}

		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.Equal(s.T(), fmt.Sprintf("%d", TEST_CONTENT_FLAGS), resp.Headers.Get("X-CB-Flags"))
		assertContentEncoding(resp.Headers.Get("Content-Encoding"))
		assert.Equal(s.T(), "application/json", resp.Headers.Get("Content-Type"))
		// we assume the content is correct, we've already checked this in other tests
		assert.Equal(s.T(), "", resp.Headers.Get("Expires"))
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
		})
	})
}

func (s *GatewayOpsTestSuite) TestDapiPost() {
	s.Run("Basic", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
			},
			Body: TEST_CONTENT,
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		})
	})

	s.Run("Compressed", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization":    s.basicRestCreds,
				"X-CB-Flags":       fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				"Content-Encoding": "snappy",
			},
			Body: s.compressContent(TEST_CONTENT),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		})
	})

	s.Run("Expiry", func() {
		docId := s.randomDocId()
		expiryTime := time.Now().Add(1 * time.Hour)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				"Expires":       expiryTime.Format(time.RFC1123),
			},
			Body: TEST_CONTENT,
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
			expiry:         expiryCheckType_Within,
			expiryBounds: expiryCheckTypeWithinBounds{
				MinSecs: 59 * 60,
				MaxSecs: 61 * 60,
			},
		})
	})

	s.Run("RelativeExpiry", func() {
		docId := s.randomDocId()
		expiryTime := 1 * time.Hour

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				"Expires":       expiryTime.String(),
			},
			Body: TEST_CONTENT,
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
			expiry:         expiryCheckType_Within,
			expiryBounds: expiryCheckTypeWithinBounds{
				MinSecs: 59 * 60,
				MaxSecs: 61 * 60,
			},
		})
	})

	s.Run("DocExists", func() {
		docId := s.testDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
			},
			Body: TEST_CONTENT,
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "DocumentExists",
			Resource: fmt.Sprintf(
				"/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
		})
	})

	s.Run("DocLocked", func() {
		docId := s.lockedDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
			},
			Body: TEST_CONTENT,
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "DocumentExists",
			Resource: fmt.Sprintf(
				"/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
		})
	})

	s.Run("ValueTooLarge", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
			},
			Body: s.largeTestContent(),
		})
		requireRestError(s.T(), resp, http.StatusRequestEntityTooLarge, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("Compressed ValueTooLarge", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization":    s.basicRestCreds,
				"X-CB-Flags":       fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				"Content-Encoding": "snappy",
			},
			Body: s.compressContent(s.largeTestRandomContent()),
		})
		requireRestError(s.T(), resp, http.StatusRequestEntityTooLarge, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("InvalidFlags", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"X-CB-Flags":    "invalid-flags",
			},
			Body: TEST_CONTENT,
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, nil)
	})

	s.Run("BlankFlags", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"X-CB-Flags":    "",
			},
			Body: TEST_CONTENT,
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, nil)
	})

	s.IterDapiDurabilityLevelTests(func(durabilityLevel string, assertFailure func(*testHttpResponse)) {
		docId := s.randomDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization":        s.basicRestCreds,
				"X-CB-Flags":           fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				"X-CB-DurabilityLevel": durabilityLevel,
			},
			Body: TEST_CONTENT,
		})
		if assertFailure != nil {
			assertFailure(resp)
			return
		}

		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		})
	})

	s.IterDapiDocumentEncodingTests(TEST_CONTENT, func(
		contentEncoding string,
		encodedContent []byte,
		assertFailure func(*testHttpResponse),
	) {
		docId := s.randomDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization":    s.basicRestCreds,
				"X-CB-Flags":       fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				"Content-Encoding": contentEncoding,
			},
			Body: encodedContent,
		})
		if assertFailure != nil {
			assertFailure(resp)
			return
		}

		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
		})
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
			Body:    TEST_CONTENT,
		})
	})
}

func (s *GatewayOpsTestSuite) TestDapiPut() {
	// These are all the test-cases that do not use If-Match
	s.Run("Upsert", func() {
		s.Run("Basic", func() {
			docId := s.randomDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				},
				Body: TEST_CONTENT,
			})
			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
			})
		})

		s.Run("Compressed", func() {
			docId := s.randomDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization":    s.basicRestCreds,
					"X-CB-Flags":       fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
					"Content-Encoding": "snappy",
				},
				Body: s.compressContent(TEST_CONTENT),
			})
			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
			})
		})

		s.Run("Expiry", func() {
			docId := s.randomDocId()
			expiryTime := time.Now().Add(1 * time.Hour)

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
					"Expires":       expiryTime.Format(time.RFC1123),
				},
				Body: TEST_CONTENT,
			})
			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MinSecs: 59 * 60,
					MaxSecs: 61 * 60,
				},
			})
		})

		s.Run("RelativeExpiry", func() {
			docId := s.randomDocId()
			expiryTime := 1 * time.Hour

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
					"Expires":       expiryTime.String(),
				},
				Body: TEST_CONTENT,
			})
			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MinSecs: 59 * 60,
					MaxSecs: 61 * 60,
				},
			})
		})

		s.Run("DocLocked", func() {
			docId := s.lockedDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				},
				Body: TEST_CONTENT,
			})
			requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
				Code: "DocumentLocked",
				Resource: fmt.Sprintf(
					"/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
			})
		})

		s.Run("ValueTooLarge", func() {
			docId := s.randomDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				},
				Body: s.largeTestContent(),
			})
			requireRestError(s.T(), resp, http.StatusRequestEntityTooLarge, &testRestError{
				Code: "InvalidArgument",
			})
		})

		s.Run("Compressed ValueTooLarge", func() {
			docId := s.randomDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization":    s.basicRestCreds,
					"X-CB-Flags":       fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
					"Content-Encoding": "snappy",
				},
				Body: s.compressContent(s.largeTestRandomContent()),
			})
			requireRestError(s.T(), resp, http.StatusRequestEntityTooLarge, &testRestError{
				Code: "InvalidArgument",
			})
		})

		s.Run("PreserveExpiry", func() {
			kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)
			expiry := 24 * time.Hour
			docId := s.randomDocId()

			upsertResp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content: &kv_v1.UpsertRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ContentFlags: TEST_CONTENT_FLAGS,
				Expiry: &kv_v1.UpsertRequest_ExpirySecs{
					ExpirySecs: uint32(expiry.Seconds()),
				},
				PreserveExpiryOnExisting: nil,
				DurabilityLevel:          nil,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), upsertResp, err)
			assertValidCas(s.T(), upsertResp.Cas)
			assertValidMutationToken(s.T(), upsertResp.MutationToken, s.bucketName)

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				},
				Body: TEST_CONTENT,
			})
			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((24 * time.Hour).Seconds()) + 1,
					MinSecs: int((23 * time.Hour).Seconds()),
				},
			})
		})

		s.Run("InvalidFlags", func() {
			docId := s.randomDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"X-CB-Flags":    "invalid-flags",
				},
				Body: TEST_CONTENT,
			})
			requireRestError(s.T(), resp, http.StatusBadRequest, nil)
		})

		s.Run("EmptyFlags", func() {
			docId := s.randomDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"X-CB-Flags":    "",
				},
				Body: TEST_CONTENT,
			})
			requireRestError(s.T(), resp, http.StatusBadRequest, nil)
		})

		s.IterDapiDurabilityLevelTests(func(durabilityLevel string, assertFailure func(*testHttpResponse)) {
			docId := s.randomDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization":        s.basicRestCreds,
					"X-CB-Flags":           fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
					"X-CB-DurabilityLevel": durabilityLevel,
				},
				Body: TEST_CONTENT,
			})
			if assertFailure != nil {
				assertFailure(resp)
				return
			}

			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
			})
		})

		s.IterDapiDocumentEncodingTests(TEST_CONTENT, func(
			contentEncoding string,
			encodedContent []byte,
			assertFailure func(*testHttpResponse),
		) {
			docId := s.randomDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization":    s.basicRestCreds,
					"X-CB-Flags":       fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
					"Content-Encoding": contentEncoding,
				},
				Body: encodedContent,
			})
			if assertFailure != nil {
				assertFailure(resp)
				return
			}

			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
			})
		})
	})

	// These are all the test-cases with If-Match
	s.Run("Replace", func() {
		newContent := []byte(`{"boo": "baz"}`)

		s.Run("Basic", func() {
			docId := s.testDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"If-Match":      "*",
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				},
				Body: newContent,
			})
			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        newContent,
				ContentFlags:   TEST_CONTENT_FLAGS,
			})
		})

		s.Run("Compressed", func() {
			docId := s.testDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization":    s.basicRestCreds,
					"If-Match":         "*",
					"X-CB-Flags":       fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
					"Content-Encoding": "snappy",
				},
				Body: s.compressContent(newContent),
			})
			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        newContent,
				ContentFlags:   TEST_CONTENT_FLAGS,
			})
		})

		s.Run("Expiry", func() {
			docId := s.testDocId()
			expiryTime := time.Now().Add(1 * time.Hour)

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"If-Match":      "*",
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
					"Expires":       expiryTime.Format(time.RFC1123),
				},
				Body: newContent,
			})
			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        newContent,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MinSecs: 59 * 60,
					MaxSecs: 61 * 60,
				},
			})
		})

		s.Run("RelativeExpiry", func() {
			docId := s.testDocId()
			expiryTime := 1 * time.Hour

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
					"Expires":       expiryTime.String(),
				},
				Body: TEST_CONTENT,
			})
			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MinSecs: 59 * 60,
					MaxSecs: 61 * 60,
				},
			})
		})

		s.Run("WithCas", func() {
			docId, docCas := s.testDocIdAndCas()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"If-Match":      fmt.Sprintf("%08x", docCas),
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				},
				Body: newContent,
			})
			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        newContent,
				ContentFlags:   TEST_CONTENT_FLAGS,
			})
		})

		s.Run("CasMismatch", func() {
			docId, docCas := s.testDocIdAndCas()
			incorrectCas := s.incorrectCas(docCas)

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"If-Match":      fmt.Sprintf("%08x", incorrectCas),
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				},
				Body: newContent,
			})
			requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
				Code: "CasMismatch",
				Resource: fmt.Sprintf(
					"/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
			})
		})

		s.Run("InvalidCas", func() {
			docId := s.randomDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"If-Match":      "zzzzz",
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				},
				Body: newContent,
			})
			requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
				Code: "InvalidArgument",
			})
		})

		s.Run("DocMissing", func() {
			docId := s.missingDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"If-Match":      "*",
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				},
				Body: newContent,
			})
			requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
				Code: "DocumentNotFound",
				Resource: fmt.Sprintf(
					"/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
			})
		})

		s.Run("DocLocked", func() {
			docId := s.lockedDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"If-Match":      "*",
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				},
				Body: newContent,
			})
			requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
				Code: "DocumentLocked",
				Resource: fmt.Sprintf(
					"/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
			})
		})

		s.Run("ValueTooLarge", func() {
			docId := s.randomDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"If-Match":      "*",
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				},
				Body: s.largeTestContent(),
			})
			requireRestError(s.T(), resp, http.StatusRequestEntityTooLarge, &testRestError{
				Code: "InvalidArgument",
			})
		})

		s.Run("Compressed ValueTooLarge", func() {
			docId := s.randomDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization":    s.basicRestCreds,
					"If-Match":         "*",
					"X-CB-Flags":       fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
					"Content-Encoding": "snappy",
				},
				Body: s.compressContent(s.largeTestRandomContent()),
			})
			requireRestError(s.T(), resp, http.StatusRequestEntityTooLarge, &testRestError{
				Code: "InvalidArgument",
			})
		})

		s.Run("PreserveExpiry", func() {
			kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)
			expiry := 24 * time.Hour
			docId := s.randomDocId()

			upsertResp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				Key:            docId,
				Content: &kv_v1.UpsertRequest_ContentUncompressed{
					ContentUncompressed: TEST_CONTENT,
				},
				ContentFlags: TEST_CONTENT_FLAGS,
				Expiry: &kv_v1.UpsertRequest_ExpirySecs{
					ExpirySecs: uint32(expiry.Seconds()),
				},
				PreserveExpiryOnExisting: nil,
				DurabilityLevel:          nil,
			}, grpc.PerRPCCredentials(s.basicRpcCreds))
			requireRpcSuccess(s.T(), upsertResp, err)
			assertValidCas(s.T(), upsertResp.Cas)
			assertValidMutationToken(s.T(), upsertResp.MutationToken, s.bucketName)

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"If-Match":      "*",
					"X-CB-Flags":    fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
				},
				Body: TEST_CONTENT,
			})
			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        TEST_CONTENT,
				ContentFlags:   TEST_CONTENT_FLAGS,
				expiry:         expiryCheckType_Within,
				expiryBounds: expiryCheckTypeWithinBounds{
					MaxSecs: int((24 * time.Hour).Seconds()) + 1,
					MinSecs: int((23 * time.Hour).Seconds()),
				},
			})
		})

		s.Run("InvalidFlags", func() {
			docId := s.testDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"If-Match":      "*",
					"X-CB-Flags":    "invalid-flags",
				},
				Body: newContent,
			})
			requireRestError(s.T(), resp, http.StatusBadRequest, nil)
		})

		s.Run("EmptyFlags", func() {
			docId := s.testDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization": s.basicRestCreds,
					"If-Match":      "*",
					"X-CB-Flags":    "",
				},
				Body: newContent,
			})
			requireRestError(s.T(), resp, http.StatusBadRequest, nil)
		})

		s.IterDapiDurabilityLevelTests(func(durabilityLevel string, assertFailure func(*testHttpResponse)) {
			docId := s.testDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization":        s.basicRestCreds,
					"If-Match":             "*",
					"X-CB-Flags":           fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
					"X-CB-DurabilityLevel": durabilityLevel,
				},
				Body: newContent,
			})
			if assertFailure != nil {
				assertFailure(resp)
				return
			}

			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        newContent,
				ContentFlags:   TEST_CONTENT_FLAGS,
			})
		})

		s.IterDapiDocumentEncodingTests(newContent, func(
			contentEncoding string,
			encodedContent []byte,
			assertFailure func(*testHttpResponse),
		) {
			docId := s.testDocId()

			resp := s.sendTestHttpRequest(&testHttpRequest{
				Method: http.MethodPut,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, docId,
				),
				Headers: map[string]string{
					"Authorization":    s.basicRestCreds,
					"If-Match":         "*",
					"X-CB-Flags":       fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
					"Content-Encoding": contentEncoding,
				},
				Body: encodedContent,
			})
			if assertFailure != nil {
				assertFailure(resp)
				return
			}

			requireRestSuccess(s.T(), resp)
			assertRestValidEtag(s.T(), resp)
			assertRestValidMutationToken(s.T(), resp, s.bucketName)

			s.checkDocument(s.T(), checkDocumentOptions{
				BucketName:     s.bucketName,
				ScopeName:      s.scopeName,
				CollectionName: s.collectionName,
				DocId:          docId,
				Content:        newContent,
				ContentFlags:   TEST_CONTENT_FLAGS,
			})
		})
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPut,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
			Body:    TEST_CONTENT,
		})
	})
}

func (s *GatewayOpsTestSuite) TestDapiDelete() {
	s.Run("Basic", func() {
		docId := s.testDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodDelete,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: nil,
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        nil,
		})
	})

	s.Run("WithCas", func() {
		docId, docCas := s.testDocIdAndCas()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodDelete,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"If-Match":      fmt.Sprintf("%08x", docCas),
			},
			Body: nil,
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        nil,
		})
	})

	s.Run("CasMismatch", func() {
		docId, docCas := s.testDocIdAndCas()
		incorrectCas := s.incorrectCas(docCas)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodDelete,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"If-Match":      fmt.Sprintf("%08x", incorrectCas),
			},
			Body: nil,
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "CasMismatch",
			Resource: fmt.Sprintf(
				"/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
		})
	})

	s.Run("InvalidCas", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodDelete,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"If-Match":      "zzzzz",
			},
			Body: nil,
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("DocMissing", func() {
		docId := s.missingDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodDelete,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf(
				"/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
		})
	})

	s.Run("DocLocked", func() {
		docId := s.lockedDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodDelete,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "DocumentLocked",
			Resource: fmt.Sprintf(
				"/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
		})
	})

	s.IterDapiDurabilityLevelTests(func(durabilityLevel string, assertFailure func(*testHttpResponse)) {
		docId := s.testDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodDelete,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization":        s.basicRestCreds,
				"X-CB-DurabilityLevel": durabilityLevel,
			},
			Body: nil,
		})
		if assertFailure != nil {
			assertFailure(resp)
			return
		}

		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        nil,
		})
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodDelete,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
		})
	})
}

func (s *GatewayOpsTestSuite) TestDapiIncrement() {
	checkDocument := func(docId string, content []byte) {
		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        content,
			ContentFlags:   0,
		})
	}

	s.Run("Basic", func() {
		docId := s.binaryDocId([]byte("5"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte("6"))
	})

	s.Run("BadContentType", func() {
		docId := s.binaryDocId([]byte("5"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"delta": 0}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, nil)
	})

	s.Run("ZeroDelta", func() {
		docId := s.binaryDocId([]byte("5"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"delta": 0}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte("5"))
	})

	s.Run("WithInitialExists", func() {
		docId := s.binaryDocId([]byte("5"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"initial": 5}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte("6"))
	})

	s.Run("WithInitialMissing", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"initial": 5}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte("5"))
	})

	s.Run("WithInitialNegative", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"initial": -2}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, nil)
	})

	s.Run("DocMissing", func() {
		docId := s.missingDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf(
				"/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
		})
	})

	s.Run("DocLocked", func() {
		docId := s.binaryDocId([]byte("5"))
		s.lockDoc(docId)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "DocumentLocked",
			Resource: fmt.Sprintf(
				"/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
		})
	})

	s.Run("NonNumericDoc", func() {
		docId := s.binaryDocId([]byte(`{"foo":"bar"}`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "DocumentNotNumeric",
			Resource: fmt.Sprintf(
				"/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
		})
	})

	s.Run("Expiry", func() {
		docId := s.randomDocId()
		expiryTime := time.Now().Add(1 * time.Hour)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
				"Expires":       expiryTime.Format(time.RFC1123),
			},
			Body: []byte(`{"initial":2}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("2"),
			ContentFlags:   0,
			expiry:         expiryCheckType_Within,
			expiryBounds: expiryCheckTypeWithinBounds{
				MinSecs: 59 * 60,
				MaxSecs: 61 * 60,
			},
		})
	})

	s.Run("ExpiryButExisting", func() {
		docId := s.binaryDocId([]byte("5"))
		expiryTime := time.Now().Add(1 * time.Hour)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
				"Expires":       expiryTime.Format(time.RFC1123),
			},
			Body: []byte(`{"initial":2}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		// expiry is not set on increment of an existing document
		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("6"),
			ContentFlags:   0,
			expiry:         expiryCheckType_None,
		})
	})

	s.Run("IllogicalExpiry", func() {
		docId := s.binaryDocId([]byte("5"))
		expiryTime := time.Now().Add(1 * time.Hour)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Expires":       expiryTime.Format(time.RFC1123),
			},
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("PreserveExpiry", func() {
		kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)
		expiry := 24 * time.Hour
		docId := s.randomDocId()

		upsertResp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: []byte(`6`),
			},
			ContentFlags: 0,
			Expiry: &kv_v1.UpsertRequest_ExpirySecs{
				ExpirySecs: uint32(expiry.Seconds()),
			},
			PreserveExpiryOnExisting: nil,
			DurabilityLevel:          nil,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), upsertResp, err)
		assertValidCas(s.T(), upsertResp.Cas)
		assertValidMutationToken(s.T(), upsertResp.MutationToken, s.bucketName)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("7"),
			ContentFlags:   0,
			expiry:         expiryCheckType_Within,
			expiryBounds: expiryCheckTypeWithinBounds{
				MaxSecs: int((24 * time.Hour).Seconds()) + 1,
				MinSecs: int((23 * time.Hour).Seconds()),
			},
		})
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
		})
	})

	s.IterDapiDurabilityLevelTests(func(durabilityLevel string, assertFailure func(*testHttpResponse)) {
		docId := s.binaryDocId([]byte("5"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization":        s.basicRestCreds,
				"X-CB-DurabilityLevel": durabilityLevel,
			},
		})
		if assertFailure != nil {
			assertFailure(resp)
			return
		}

		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("6"),
			ContentFlags:   0,
		})
	})
}

func (s *GatewayOpsTestSuite) TestDapiDecrement() {
	checkDocument := func(docId string, content []byte) {
		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        content,
			ContentFlags:   0,
		})
	}

	s.Run("Basic", func() {
		docId := s.binaryDocId([]byte("5"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte("4"))
	})

	s.Run("BadContentType", func() {
		docId := s.binaryDocId([]byte("5"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"delta": 0}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, nil)
	})

	s.Run("ZeroDelta", func() {
		docId := s.binaryDocId([]byte("5"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"delta": 0}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte("5"))
	})

	s.Run("WithInitialExists", func() {
		docId := s.binaryDocId([]byte("5"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"initial": 5}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte("4"))
	})

	s.Run("WithInitialMissing", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"initial": 5}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte("5"))
	})

	s.Run("WithInitialNegative", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"initial": -2}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, nil)
	})

	s.Run("DocMissing", func() {
		docId := s.missingDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf(
				"/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
		})
	})

	s.Run("DocLocked", func() {
		docId := s.binaryDocId([]byte("5"))
		s.lockDoc(docId)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "DocumentLocked",
			Resource: fmt.Sprintf(
				"/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
		})
	})

	s.Run("NonNumericDoc", func() {
		docId := s.binaryDocId([]byte(`{"foo":"bar"}`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "DocumentNotNumeric",
			Resource: fmt.Sprintf(
				"/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
		})
	})

	s.Run("Expiry", func() {
		docId := s.randomDocId()
		expiryTime := time.Now().Add(1 * time.Hour)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
				"Expires":       expiryTime.Format(time.RFC1123),
			},
			Body: []byte(`{"initial":2}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("2"),
			ContentFlags:   0,
			expiry:         expiryCheckType_Within,
			expiryBounds: expiryCheckTypeWithinBounds{
				MinSecs: 59 * 60,
				MaxSecs: 61 * 60,
			},
		})
	})

	s.Run("ExpiryButExisting", func() {
		docId := s.binaryDocId([]byte("5"))
		expiryTime := time.Now().Add(1 * time.Hour)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
				"Expires":       expiryTime.Format(time.RFC1123),
			},
			Body: []byte(`{"initial":2}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		// expiry is not set on increment of an existing document
		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("4"),
			ContentFlags:   0,
			expiry:         expiryCheckType_None,
		})
	})

	s.Run("IllogicalExpiry", func() {
		docId := s.binaryDocId([]byte("5"))
		expiryTime := time.Now().Add(1 * time.Hour)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Expires":       expiryTime.Format(time.RFC1123),
			},
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("PreserveExpiry", func() {
		kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)
		expiry := 24 * time.Hour
		docId := s.randomDocId()

		upsertResp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: []byte(`6`),
			},
			ContentFlags: 0,
			Expiry: &kv_v1.UpsertRequest_ExpirySecs{
				ExpirySecs: uint32(expiry.Seconds()),
			},
			PreserveExpiryOnExisting: nil,
			DurabilityLevel:          nil,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), upsertResp, err)
		assertValidCas(s.T(), upsertResp.Cas)
		assertValidMutationToken(s.T(), upsertResp.MutationToken, s.bucketName)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("5"),
			ContentFlags:   0,
			expiry:         expiryCheckType_Within,
			expiryBounds: expiryCheckTypeWithinBounds{
				MaxSecs: int((24 * time.Hour).Seconds()) + 1,
				MinSecs: int((23 * time.Hour).Seconds()),
			},
		})
	})

	s.IterDapiDurabilityLevelTests(func(durabilityLevel string, assertFailure func(*testHttpResponse)) {
		docId := s.binaryDocId([]byte("5"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization":        s.basicRestCreds,
				"X-CB-DurabilityLevel": durabilityLevel,
			},
		})
		if assertFailure != nil {
			assertFailure(resp)
			return
		}

		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("4"),
			ContentFlags:   0,
		})
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
		})
	})
}

func (s *GatewayOpsTestSuite) TestDapiLookupIn() {
	s.Run("Basic", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"Get","path":"obj.num"},
				{"operation":"Get","path":"arr"},
				{"operation":"Exists","path":"arr"},
				{"operation":"Exists","path":"missing"},
				{"operation":"GetCount","path":"arr"}
			]}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.Equal(s.T(), "application/json", resp.Headers.Get("Content-Type"))
		assert.JSONEq(s.T(), `[
			{"value":14},
			{"value":[3,6,9,12]},
			{"value":true},
			{"value":false},
			{"value":4}
		]`, string(resp.Body))
	})

	s.Run("PathNotFound", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"Get","path":"obj.num"},
				{"operation":"Get","path":"missing"}
			]}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.JSONEq(s.T(), `[
			{"value":14},
			{"error":{"error":"PathNotFound", 
				"message":"The requested path was not found in the document."}}
		]`, string(resp.Body))
	})

	s.Run("PathInvalid", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"Get","path":"obj.num"},
				{"operation":"Get","path":"bad..path"}
			]}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.JSONEq(s.T(), `[
			{"value":14},
			{"error":{"error":"InvalidArgument", 
				"message":"Invalid path specified."}}
		]`, string(resp.Body))
	})

	s.Run("PathMismatch", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"Get","path":"obj.num"},
				{"operation":"Get","path":"obj.num.x"}
			]}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.JSONEq(s.T(), `[
			{"value":14},
			{"error":{"error":"PathMismatch", 
				"message":"The structure implied by the provided path does not match the document."}}
		]`, string(resp.Body))
	})

	s.Run("PathTooBig", func() {
		path := strings.Repeat(".a", 64)[1:]

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"Get","path":"obj.num"},
				{"operation":"Get","path":"` + path + `"}
			]}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.JSONEq(s.T(), `[
			{"value":14},
			{"error":{"error":"InvalidArgument", 
				"message":"The specified path was too big."}}
		]`, string(resp.Body))
	})

	s.Run("DocLocked", func() {
		docId := s.lockedDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				`/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup`,
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[{"operation":"Get","path":"arr"}]}`),
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "DocumentLocked",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("DocMissing", func() {
		docId := s.missingDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				`/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup`,
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[{"operation":"Get","path":"arr"}]}`),
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("NonJsonDoc", func() {
		docId := s.binaryDocId([]byte(`hello-world`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"Get","path":"obj.num"}
			]}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "DocumentNotJson",
			Resource: fmt.Sprintf(
				"/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
		})
	})

	s.Run("ArrayElement", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"Get","path":"obj.arr[1]"}
			]}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.Equal(s.T(), "application/json", resp.Headers.Get("Content-Type"))
		assert.JSONEq(s.T(), `[
			{"value":5}
		]`, string(resp.Body))
	})

	s.Run("InvalidJsonPayload", func() {
		docId := s.testDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"Get","path":"obj.arr[1]"},
			]}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, nil)
	})

	// // ING-1102
	// s.Run("Empty Path", func() {
	// 	resp := s.sendTestHttpRequest(&testHttpRequest{
	// 		Method: http.MethodPost,
	// 		Path: fmt.Sprintf(
	// 			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
	// 			s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
	// 		),
	// 		Headers: map[string]string{
	// 			"Authorization": s.basicRestCreds,
	// 		},
	// 		Body: []byte(`{"operations":[
	// 			{"operation":"Get","path":""}
	// 		]}`),
	// 	})
	// 	requireRestSuccess(s.T(), resp)
	// 	assertRestValidEtag(s.T(), resp)
	// 	assert.Equal(s.T(), "application/json", resp.Headers.Get("Content-Type"))
	// 	assert.JSONEq(s.T(), string(TEST_CONTENT), string(resp.Body))
	// })

	// ING-1102
	// s.Run("No Path", func() {
	// 	resp := s.sendTestHttpRequest(&testHttpRequest{
	// 		Method: http.MethodPost,
	// 		Path: fmt.Sprintf(
	// 			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
	// 			s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
	// 		),
	// 		Headers: map[string]string{
	// 			"Authorization": s.basicRestCreds,
	// 		},
	// 		Body: []byte(`{"operations":[
	// 			{"operation":"Get","path":""}
	// 		]}`),
	// 	})
	// 	requireRestSuccess(s.T(), resp)
	// 	assertRestValidEtag(s.T(), resp)
	// 	assert.Equal(s.T(), "application/json", resp.Headers.Get("Content-Type"))
	// 	assert.JSONEq(s.T(), string(TEST_CONTENT), string(resp.Body))
	// })

	s.Run("Too Many Operations", func() {
		docId := s.testDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"},
				{"operation":"Get","path":"obj"}
			]}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	// ING-1106
	// s.Run("No Operations", func() {
	// 	docId := s.testDocId()
	// 	resp := s.sendTestHttpRequest(&testHttpRequest{
	// 		Method: http.MethodPost,
	// 		Path: fmt.Sprintf(
	// 			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
	// 			s.bucketName, s.scopeName, s.collectionName, docId,
	// 		),
	// 		Headers: map[string]string{
	// 			"Authorization": s.basicRestCreds,
	// 		},
	// 		Body: []byte(`{"operations":[]}`),
	// 	})
	// 	requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
	// 		Code: "InvalidArgument",
	// 	})
	// })

	// ING-1106
	// s.Run("Missing Operations", func() {
	// 	docId := s.testDocId()
	// 	resp := s.sendTestHttpRequest(&testHttpRequest{
	// 		Method: http.MethodPost,
	// 		Path: fmt.Sprintf(
	// 			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
	// 			s.bucketName, s.scopeName, s.collectionName, docId,
	// 		),
	// 		Headers: map[string]string{
	// 			"Authorization": s.basicRestCreds,
	// 		},
	// 		Body: []byte(`{}`),
	// 	})
	// 	requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
	// 		Code: "InvalidArgument",
	// 	})
	// })

	s.Run("Doc too deep", func() {
		var depth int
		body := make(map[string]interface{})

		var addToBody func(map[string]interface{})
		addToBody = func(b map[string]interface{}) {
			depth++
			if depth > 64 {
				return
			}

			b["a"] = make(map[string]interface{})
			current := b["a"].(map[string]interface{})
			addToBody(current)
		}
		addToBody(body)

		b, _ := json.Marshal(body)

		docId := s.binaryDocId(b)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"Get","path":"a"}
			]}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "DocumentTooDeep",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lookup",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
			Body:    []byte(`{"operations":[{"operation":"Get","path":"arr"}]}`),
		})
	})
}

func (s *GatewayOpsTestSuite) TestDapiMutateIn() {
	checkDocument := func(docId string, content []byte) {
		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        content,
			ContentFlags:   0,
			CheckAsJson:    true,
		})
	}

	s.Run("Basic", func() {
		docId := s.binaryDocId([]byte(`{
			"num":14,
			"rep":16,
			"arr":[3,6,9,12],
			"ctr":3,
			"rem":true
		}`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"DictSet","path":"num", "value": 42},
				{"operation":"DictAdd","path":"add", "value": 43},
				{"operation":"Replace","path":"rep", "value": 44},
				{"operation":"Delete","path":"rem"},
				{"operation":"ArrayPushLast","path":"arr", "value": 42},
				{"operation":"ArrayPushFirst","path":"arr", "value": 99},
				{"operation":"ArrayAddUnique","path":"arr", "value": 74},
				{"operation":"ArrayInsert","path":"arr[2]", "value": 77},
				{"operation":"Counter","path":"ctr", "value": 2}
			]}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)

		checkDocument(docId, []byte(`{
			"num":42,
			"add":43,
			"rep":44,
			"arr":[99,3,77,6,9,12,42,74],
			"ctr":5
		}`))
	})

	s.Run("DictAddPathExists", func() {
		docId := s.binaryDocId([]byte(`{
			"add":14
		}`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"DictAdd","path":"add", "value": 43}
			]}`),
		})

		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "PathExists",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s/content/{add}",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("DictReplacePathNotFound", func() {
		docId := s.binaryDocId([]byte(`{}`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"Replace","path":"rep", "value": 43}
			]}`),
		})

		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "PathNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s/content/{rep}",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("PathMismatch", func() {
		docId := s.binaryDocId([]byte(`{"x":14}`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"DictSet","path":"x.y.z", "value": 43}
			]}`),
		})

		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "PathMismatch",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s/content/{x.y.z}",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("ArrayPushLastOnNonArray", func() {
		docId := s.binaryDocId([]byte(`{"x":14}`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"ArrayPushLast","path":"x", "value": 43}
			]}`),
		})

		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "PathMismatch",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s/content/{x}",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("CounterOnNonNumber", func() {
		docId := s.binaryDocId([]byte(`{"x":"y"}`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"Counter","path":"x", "value": 1}
			]}`),
		})

		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "PathMismatch",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s/content/{x}",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("PathInvalid", func() {
		docId := s.binaryDocId([]byte(`{}`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"DictSet","path":"bad..path", "value": 43}
			]}`),
		})

		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("PathTooBig", func() {
		path := strings.Repeat(".a", 64)[1:]
		docId := s.binaryDocId([]byte(`{}`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"DictSet","path":"` + path + `", "value": 43}
			]}`),
		})

		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("UpsertSemanticReplace", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{
				  "storeSemantic": "Upsert",
				  "operations":[
				    {"operation":"Replace","path":"test", "value": 43}
				]}`),
		})

		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("DocLocked", func() {
		docId := s.lockedDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				`/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate`,
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"DictSet","path":"x", "value": 43}
			]}`),
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "DocumentLocked",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("DocMissing", func() {
		docId := s.missingDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				`/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate`,
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
					{"operation":"DictSet","path":"x", "value": 43}
				]}`),
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("NonJsonDoc", func() {
		docId := s.binaryDocId([]byte(`hello-world`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
					{"operation":"DictSet","path":"x", "value": 43}
				]}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "DocumentNotJson",
			Resource: fmt.Sprintf(
				"/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
		})
	})

	s.Run("InvalidJsonPayload", func() {
		docId := s.testDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
					{"operation":"DictSet","path":"x", "value": 43},
			]}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, nil)
	})

	// ING-1102
	// s.Run("Empty Path", func() {
	// 	resp := s.sendTestHttpRequest(&testHttpRequest{
	// 		Method: http.MethodPost,
	// 		Path: fmt.Sprintf(
	// 			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
	// 			s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
	// 		),
	// 		Headers: map[string]string{
	// 			"Authorization": s.basicRestCreds,
	// 		},
	// 		Body: []byte(`{"operations":[
	// 				{"operation":"DictSet","path":"","value": 43}
	// 		]}`),
	// 	})
	// requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
	// 	Code: "InvalidArgument",
	// })
	// })

	// ING-1102
	// s.Run("No Path", func() {
	// 	resp := s.sendTestHttpRequest(&testHttpRequest{
	// 		Method: http.MethodPost,
	// 		Path: fmt.Sprintf(
	// 			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
	// 			s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
	// 		),
	// 		Headers: map[string]string{
	// 			"Authorization": s.basicRestCreds,
	// 		},
	// 		Body: []byte(`{"operations":[
	// 				{"operation":"DictSet","value": 43}
	// 		]}`),
	// 	})
	// requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
	// 	Code: "InvalidArgument",
	// })
	// })

	// ING-1105
	// s.Run("No Value", func() {
	// 	resp := s.sendTestHttpRequest(&testHttpRequest{
	// 		Method: http.MethodPost,
	// 		Path: fmt.Sprintf(
	// 			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
	// 			s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
	// 		),
	// 		Headers: map[string]string{
	// 			"Authorization": s.basicRestCreds,
	// 		},
	// 		Body: []byte(`{"operations":[
	// 				{"operation":"DictSet","path":"x"}
	// 		]}`),
	// 	})
	// requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
	// 	Code: "InvalidArgument",
	// })
	// })

	s.Run("Too Many Operations", func() {
		docId := s.testDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43},
				{"operation":"DictSet","path":"x", "value": 43}
			]}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("No Operations", func() {
		docId := s.testDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"operations":[]}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("Missing Operations", func() {
		docId := s.testDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.IterDapiDurabilityLevelTests(func(durabilityLevel string, assertFailure func(*testHttpResponse)) {
		docId := s.binaryDocId([]byte(`{
					"num":14,
					"rep":16,
					"arr":[3,6,9,12],
					"ctr":3,
					"rem":true
				}`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization":        s.basicRestCreds,
				"X-CB-DurabilityLevel": durabilityLevel,
			},
			Body: []byte(`{"operations":[
						{"operation":"DictSet","path":"num","value": 42}
					]}`),
		})
		if assertFailure != nil {
			assertFailure(resp)
			return
		}

		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		// ING-1104
		// assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte(`{
					"num":42,
					"rep":16,
					"arr":[3,6,9,12],
					"ctr":3,
					"rem":true
				}`))
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
			Body: []byte(`{"operations":[
					{"operation":"DictSet","path":"x", "value": 43}
				]}`),
		})
	})
}

func (s *GatewayOpsTestSuite) TestDapiCors() {
	docId := s.randomDocId()

	resp := s.sendTestHttpRequest(&testHttpRequest{
		Method: http.MethodOptions,
		Path: fmt.Sprintf(
			"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
			s.bucketName, s.scopeName, s.collectionName, docId,
		),
		Headers: map[string]string{
			"Authorization":                 s.basicRestCreds,
			"Access-Control-Request-Method": http.MethodPut,
			"Origin":                        "http://example.com",
		},
	})
	require.NotNil(s.T(), resp)
	requireRestSuccessNoContent(s.T(), resp)
	assert.Equal(s.T(), "*",
		resp.Headers.Get("Access-Control-Allow-Origin"))

	resp = s.sendTestHttpRequest(&testHttpRequest{
		Method: http.MethodPut,
		Path: fmt.Sprintf(
			"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
			s.bucketName, s.scopeName, s.collectionName, docId,
		),
		Headers: map[string]string{
			"Authorization": s.basicRestCreds,
			"Origin":        "http://example.com",
		},
		Body: TEST_CONTENT,
	})
	requireRestSuccess(s.T(), resp)
	assert.Equal(s.T(), "*",
		resp.Headers.Get("Access-Control-Allow-Origin"))

	resp = s.sendTestHttpRequest(&testHttpRequest{
		Method: http.MethodGet,
		Path: fmt.Sprintf(
			"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
			s.bucketName, s.scopeName, s.collectionName, docId,
		),
		Headers: map[string]string{
			"Authorization": s.basicRestCreds,
			"Origin":        "http://example.com",
		},
	})
	requireRestSuccess(s.T(), resp)
	assert.Equal(s.T(), "*",
		resp.Headers.Get("Access-Control-Allow-Origin"))
}

func (s *GatewayOpsTestSuite) TestDapiAppend() {
	checkDocument := func(docId string, content []byte) {
		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        content,
			ContentFlags:   0,
		})
	}

	s.Run("Basic", func() {
		docId := s.binaryDocId([]byte("abcde"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`fghi`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte("abcdefghi"))
	})

	s.Run("WithCas", func() {
		docId := s.randomDocId()
		cas := s.createDocument(createDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("abcde"),
			ContentFlags:   0,
		})

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"If-Match":      fmt.Sprintf("%08x", cas),
			},
			Body: []byte(`fghi`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte("abcdefghi"))
	})

	s.Run("WithWrongCas", func() {
		docId := s.binaryDocId([]byte("abcde"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"If-Match":      fmt.Sprintf("%08x", 12345),
			},
			Body: []byte(`fghi`),
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "CasMismatch",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("DocMissing", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`fghi`),
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("DocToolarge", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: s.largeTestContent(),
		})
		requireRestError(s.T(), resp, http.StatusRequestEntityTooLarge, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("MissingBody", func() {
		docId := s.binaryDocId([]byte("abcde"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.IterDapiDurabilityLevelTests(func(durabilityLevel string, assertFailure func(*testHttpResponse)) {
		docId := s.binaryDocId([]byte(`abcde`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization":        s.basicRestCreds,
				"X-CB-DurabilityLevel": durabilityLevel,
			},
			Body: []byte(`fghi`),
		})
		if assertFailure != nil {
			assertFailure(resp)
			return
		}

		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte(`abcdefghi`))
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
			Body:    []byte(`fghi`),
		})
	})
}

func (s *GatewayOpsTestSuite) TestDapiPrepend() {
	checkDocument := func(docId string, content []byte) {
		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        content,
			ContentFlags:   0,
		})
	}

	s.Run("Basic", func() {
		docId := s.binaryDocId([]byte("fghi"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`abcde`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte("abcdefghi"))
	})

	s.Run("WithCas", func() {
		docId := s.randomDocId()
		cas := s.createDocument(createDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        []byte("fghi"),
			ContentFlags:   0,
		})

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"If-Match":      fmt.Sprintf("%08x", cas),
			},
			Body: []byte(`abcde`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte("abcdefghi"))
	})

	s.Run("WithWrongCas", func() {
		docId := s.binaryDocId([]byte("fghi"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"If-Match":      fmt.Sprintf("%08x", 12345),
			},
			Body: []byte(`abcde`),
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "CasMismatch",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("DocMissing", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`abcde`),
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("DocToolarge", func() {
		docId := s.randomDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: s.largeTestContent(),
		})
		requireRestError(s.T(), resp, http.StatusRequestEntityTooLarge, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("MissingBody", func() {
		docId := s.binaryDocId([]byte("abcde"))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.IterDapiDurabilityLevelTests(func(durabilityLevel string, assertFailure func(*testHttpResponse)) {
		docId := s.binaryDocId([]byte(`fghi`))

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization":        s.basicRestCreds,
				"X-CB-DurabilityLevel": durabilityLevel,
			},
			Body: []byte(`abcde`),
		})
		if assertFailure != nil {
			assertFailure(resp)
			return
		}

		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		checkDocument(docId, []byte(`abcdefghi`))
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
			Body:    []byte(`fghi`),
		})
	})
}

func (s *GatewayOpsTestSuite) TestDapiTouch() {
	s.Run("Basic", func() {
		docId := s.testDocId()
		expiryTime := time.Now().Add(1 * time.Hour)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(fmt.Sprintf(`{"expiry":"%s"}`, expiryTime.Format(time.RFC1123))),
		})
		require.NotNil(s.T(), resp)
		require.Equal(s.T(), http.StatusNoContent, resp.StatusCode, "status code was not 204")

		assertRestValidEtag(s.T(), resp)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
			expiry:         expiryCheckType_Within,
			expiryBounds: expiryCheckTypeWithinBounds{
				MinSecs: 59 * 60,
				MaxSecs: 61 * 60,
			},
		})
	})

	s.Run("ReturnContent", func() {
		docId := s.testDocId()
		expiryTime := time.Now().Add(1 * time.Hour)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(fmt.Sprintf(`{"expiry":"%s","returnContent":true}`, expiryTime.Format(time.RFC1123))),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)

		assert.Equal(s.T(), fmt.Sprintf("%d", TEST_CONTENT_FLAGS), resp.Headers.Get("X-CB-Flags"))
		assert.Equal(s.T(), "", resp.Headers.Get("Content-Encoding"))
		assert.Equal(s.T(), "application/json", resp.Headers.Get("Content-Type"))
		assert.Equal(s.T(), TEST_CONTENT, resp.Body)
		assert.Equal(s.T(), "", resp.Headers.Get("Expires"))

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
			expiry:         expiryCheckType_Within,
			expiryBounds: expiryCheckTypeWithinBounds{
				MinSecs: 59 * 60,
				MaxSecs: 61 * 60,
			},
		})
	})

	s.Run("ExpiryInPastWithReturnContent", func() {
		docId := s.testDocId()
		expiryTime := time.Now().Add(-1 * time.Hour)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(fmt.Sprintf(`{"expiry":"%s","returnContent":true}`, expiryTime.Format(time.RFC1123))),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        nil,
			ContentFlags:   0,
		})
	})

	// ING-1135
	// s.Run("NoBody", func() {
	// 	docId := s.testDocId()
	//
	// 	resp := s.sendTestHttpRequest(&testHttpRequest{
	// 		Method: http.MethodPost,
	// 		Path: fmt.Sprintf(
	// 			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
	// 			s.bucketName, s.scopeName, s.collectionName, docId,
	// 		),
	// 		Headers: map[string]string{
	// 			"Authorization": s.basicRestCreds,
	// 		},
	// 	})
	// 	requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
	// 		Code: "InvalidArgument",
	// 	})
	// })

	// ING-1135
	// s.Run("StringBody", func() {
	// 	docId := s.testDocId()
	//
	// 	resp := s.sendTestHttpRequest(&testHttpRequest{
	// 		Method: http.MethodPost,
	// 		Path: fmt.Sprintf(
	// 			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
	// 			s.bucketName, s.scopeName, s.collectionName, docId,
	// 		),
	// 		Headers: map[string]string{
	// 			"Authorization": s.basicRestCreds,
	// 		},
	// 		Body: []byte(`abcde`),
	// 	})
	// 	requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
	// 		Code: "InvalidArgument",
	// 	})
	// })

	s.Run("ZeroExpiry", func() {
		kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)
		expiry := 24 * time.Hour
		docId := s.testDocId()

		upsertResp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            docId,
			Content: &kv_v1.UpsertRequest_ContentUncompressed{
				ContentUncompressed: TEST_CONTENT,
			},
			ContentFlags: TEST_CONTENT_FLAGS,
			Expiry: &kv_v1.UpsertRequest_ExpirySecs{
				ExpirySecs: uint32(expiry.Seconds()),
			},
			PreserveExpiryOnExisting: nil,
			DurabilityLevel:          nil,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), upsertResp, err)
		assertValidCas(s.T(), upsertResp.Cas)
		assertValidMutationToken(s.T(), upsertResp.MutationToken, s.bucketName)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"expiry":"0"}`),
		})
		require.NotNil(s.T(), resp)
		require.Equal(s.T(), http.StatusNoContent, resp.StatusCode, "status code was not 204")

		assertRestValidEtag(s.T(), resp)

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
			expiry:         expiryCheckType_None,
		})
	})

	s.Run("NoExpiry", func() {
		docId := s.testDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"returnContent":true}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("EmptyObject", func() {
		docId := s.testDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{}`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidArgument",
		})
	})

	s.Run("InvalidJson", func() {
		docId := s.testDocId()

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{`),
		})
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{})
	})

	// ING-1135
	// s.Run("InvalidExpiry", func() {
	// 	docId := s.testDocId()
	//
	// 	resp := s.sendTestHttpRequest(&testHttpRequest{
	// 		Method: http.MethodPost,
	// 		Path: fmt.Sprintf(
	// 			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
	// 			s.bucketName, s.scopeName, s.collectionName, docId,
	// 		),
	// 		Headers: map[string]string{
	// 			"Authorization": s.basicRestCreds,
	// 		},
	// 		Body: []byte(`{"expiry":"invalid"}`),
	// 	})
	// 	requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
	// 		Code: "InvalidArgument",
	// 	})
	// })

	// ING-1135
	// s.Run("InvalidReturnContent", func() {
	// 	docId := s.testDocId()
	// 	expiryTime := time.Now().Add(1 * time.Hour)
	//
	// 	resp := s.sendTestHttpRequest(&testHttpRequest{
	// 		Method: http.MethodPost,
	// 		Path: fmt.Sprintf(
	// 			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
	// 			s.bucketName, s.scopeName, s.collectionName, docId,
	// 		),
	// 		Headers: map[string]string{
	// 			"Authorization": s.basicRestCreds,
	// 		},
	// 		Body: []byte(fmt.Sprintf(`{"expiry":"%s","returnContent":"invalid"}`, expiryTime.Format(time.RFC1123))),
	// 	})
	// 	requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
	// 		Code: "InvalidArgument",
	// 	})
	// })

	s.Run("RelativeExpiry", func() {
		docId := s.testDocId()
		expiryTime := 1 * time.Hour

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(fmt.Sprintf(`{"expiry":"%s"}`, expiryTime.String())),
		})
		require.NotNil(s.T(), resp)
		require.Equal(s.T(), http.StatusNoContent, resp.StatusCode, "status code was not 204")

		s.checkDocument(s.T(), checkDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          docId,
			Content:        TEST_CONTENT,
			ContentFlags:   TEST_CONTENT_FLAGS,
			expiry:         expiryCheckType_Within,
			expiryBounds: expiryCheckTypeWithinBounds{
				MinSecs: 59 * 60,
				MaxSecs: 61 * 60,
			},
		})
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
			Body:    TEST_CONTENT,
		})
	})
}
