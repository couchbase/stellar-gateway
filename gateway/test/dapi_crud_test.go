package test

import (
	"fmt"
	"net/http"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type commonDapiErrorTestData struct {
	ScopeName      string
	BucketName     string
	CollectionName string
	DocumentKey    string
	Headers        map[string]string
}

func (s *GatewayOpsTestSuite) RunCommonDapiErrorCases(
	fn func(opts *commonDapiErrorTestData) *testHttpResponse,
) {
	s.Run("CollectionMissing", func() {
		resp := fn(&commonDapiErrorTestData{
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
		resp := fn(&commonDapiErrorTestData{
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
		resp := fn(&commonDapiErrorTestData{
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
		resp := fn(&commonDapiErrorTestData{
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
		resp := fn(&commonDapiErrorTestData{
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
		resp := fn(&commonDapiErrorTestData{
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
		assert.Equal(s.T(), TEST_CONTENT, resp.Body)
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

	s.RunCommonDapiErrorCases(func(opts *commonDapiErrorTestData) *testHttpResponse {
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

	s.RunCommonDapiErrorCases(func(opts *commonDapiErrorTestData) *testHttpResponse {
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
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiErrorTestData) *testHttpResponse {
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

	s.RunCommonDapiErrorCases(func(opts *commonDapiErrorTestData) *testHttpResponse {
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

	s.RunCommonDapiErrorCases(func(opts *commonDapiErrorTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
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

	s.RunCommonDapiErrorCases(func(opts *commonDapiErrorTestData) *testHttpResponse {
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
