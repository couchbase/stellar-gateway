package test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"strconv"
)

func (s *GatewayOpsTestSuite) TestDapiAlphaEnabled() {
	resp := s.sendTestHttpRequest(&testHttpRequest{
		Method: http.MethodGet,
		Path:   "/v1.alpha/enabled",
	})
	requireRestSuccess(s.T(), resp)
}

func (s *GatewayOpsTestSuite) insertBinaryDocument() string {
	docId := s.randomDocId()
	resp := s.sendTestHttpRequest(&testHttpRequest{
		Method: http.MethodPost,
		Path: fmt.Sprintf(
			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
			s.bucketName, s.scopeName, s.collectionName, docId,
		),
		Headers: map[string]string{
			"Authorization": s.basicRestCreds,
		},
		Body: TEST_BINARY_CONTENT,
	})
	requireRestSuccess(s.T(), resp)
	assertRestValidEtag(s.T(), resp)
	assertRestValidMutationToken(s.T(), resp, s.bucketName)

	return docId
}

func (s *GatewayOpsTestSuite) TestAppendToDocument() {
	s.Run("Basic", func() {
		docId := s.insertBinaryDocument()

		// Append to the document
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: TEST_APPEND_CONTENT,
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		// Verify the key-value is present
		resp = s.sendTestHttpRequest(&testHttpRequest{
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
		println("Response body is: ", string(resp.Body))
		assert.Contains(s.T(), string(resp.Body), string(TEST_APPEND_CONTENT))
	})

	s.Run("Forbidden", func() {
		docId := s.insertBinaryDocument()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.badRestCreds,
			},
			Body: TEST_CONTENT,
		})
		requireRestError(s.T(), resp, http.StatusForbidden, &testRestError{
			Code: "InvalidAuth",
		})
	})

	s.Run("NotFound", func() {
		docId := "missing-append-to-document"
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: TEST_CONTENT,
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("Conflict", func() {
		docId := s.lockedDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: TEST_CONTENT,
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "DocumentLocked",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiErrorTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
			Body:    TEST_CONTENT,
		})
	})
}

func (s *GatewayOpsTestSuite) TestPrependToDocument() {
	s.Run("Basic", func() {
		docId := s.insertBinaryDocument()

		// Append to the document
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: TEST_APPEND_CONTENT,
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		// Verify the key-value is present
		resp = s.sendTestHttpRequest(&testHttpRequest{
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
		assert.Contains(s.T(), string(resp.Body), string(TEST_APPEND_CONTENT))
	})

	s.Run("Forbidden", func() {
		docId := s.testDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.badRestCreds,
			},
			Body: TEST_CONTENT,
		})
		requireRestError(s.T(), resp, http.StatusForbidden, &testRestError{
			Code: "InvalidAuth",
		})
	})

	s.Run("NotFound", func() {
		docId := "missing-prepend-to-document"
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: TEST_CONTENT,
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("Conflict", func() {
		docId := s.lockedDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: TEST_CONTENT,
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "DocumentLocked",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiErrorTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
			Body:    TEST_CONTENT,
		})
	})
}

func (s *GatewayOpsTestSuite) TestIncrementToDocument() {
	s.Run("Basic", func() {
		docId := "increment-to-document" + s.randomDocId()

		// Insert a binary document
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"initial":11, "delta": 0}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		// increment to the document
		resp = s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"initial":11, "delta": 100}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		// Verify the key-value is present
		resp = s.sendTestHttpRequest(&testHttpRequest{
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
		assert.Contains(s.T(), string(resp.Body), string([]byte("111")))
	})

	s.Run("Forbidden", func() {
		docId := s.testDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.badRestCreds,
			},
			Body: []byte(`{"initial":11, "delta": 100}`),
		})
		requireRestError(s.T(), resp, http.StatusForbidden, &testRestError{
			Code: "InvalidAuth",
		})
	})

	s.Run("NotFound", func() {
		docId := "missing-increment-document"
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"initial":11, "delta": 100}`),
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("Conflict", func() {
		docId := s.lockedDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"initial":11, "delta": 100}`),
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "DocumentLocked",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
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
			Body:    TEST_BINARY_CONTENT,
		})
	})
}

func (s *GatewayOpsTestSuite) TestDecrementToDocument() {
	s.Run("Basic", func() {
		docId := "decrement-to-document" + s.randomDocId()

		// Insert a binary document
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"initial":111, "delta": 0}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		// increment to the document
		resp = s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"initial":111, "delta": 11}`),
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assertRestValidMutationToken(s.T(), resp, s.bucketName)

		// Verify the key-value is present
		resp = s.sendTestHttpRequest(&testHttpRequest{
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
		assert.Contains(s.T(), string(resp.Body), string([]byte("100")))
	})

	s.Run("Forbidden", func() {
		docId := s.testDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.badRestCreds,
			},
			Body: []byte(`{"initial":11, "delta": 100}`),
		})
		requireRestError(s.T(), resp, http.StatusForbidden, &testRestError{
			Code: "InvalidAuth",
		})
	})

	s.Run("NotFound", func() {
		docId := "missing-decrement-document"
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"delta": 100}`),
		})
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("Conflict", func() {
		docId := s.lockedDocId()
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: []byte(`{"delta": 100}`),
		})
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "DocumentLocked",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
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
			Body:    []byte(`{"initial":11, "delta": 100}`),
		})
	})
}

func (s *GatewayOpsTestSuite) TestLockDocument() {
	// Helper function to create the request path
	createLockPath := func(docId string) string {
		return fmt.Sprintf(
			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lock",
			s.bucketName, s.scopeName, s.collectionName, docId,
		)
	}

	// Helper function to send a request and return the response
	sendLockRequest := func(docId string, auth string, body []byte) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   createLockPath(docId),
			Headers: map[string]string{
				"Authorization": auth,
			},
			Body: body,
		})
	}

	s.Run("Basic", func() {
		docId := s.testDocId()
		resp := sendLockRequest(docId, s.basicRestCreds, TEST_LOCK_CONTENT)
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
	})

	s.Run("BadRequest", func() {
		docId := s.testDocId()
		resp := sendLockRequest(docId, s.basicRestCreds, []byte(`{"invalid": 100`))
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "BadRequest",
		})
	})

	s.Run("Forbidden", func() {
		docId := s.testDocId()
		resp := sendLockRequest(docId, s.badRestCreds, TEST_LOCK_CONTENT)
		requireRestError(s.T(), resp, http.StatusForbidden, &testRestError{
			Code: "InvalidAuth",
		})
	})

	s.Run("NotFound", func() {
		docId := "missing-locked-document"
		resp := sendLockRequest(docId, s.basicRestCreds, TEST_LOCK_CONTENT)
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("Conflict", func() {
		docId := s.lockedDocId()
		resp := sendLockRequest(docId, s.basicRestCreds, TEST_LOCK_CONTENT)
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "DocumentLocked",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.RunCommonDapiErrorCases(func(opts *commonDapiErrorTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lock",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: opts.Headers,
			Body:    TEST_LOCK_CONTENT,
		})
	})
}

func (s *GatewayOpsTestSuite) TestUnlockDocument() {
	// Helper function to create the request path
	docId := s.testDocId()
	createUnlockPath := func(docId string) string {
		return fmt.Sprintf(
			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/unlock",
			s.bucketName, s.scopeName, s.collectionName, docId,
		)
	}

	createLockPath := func(docId string) string {
		return fmt.Sprintf(
			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/lock",
			s.bucketName, s.scopeName, s.collectionName, docId,
		)
	}

	// Helper function to send a request and return the response
	sendUnlockRequest := func(docId string, auth string, casUint uint64) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   createUnlockPath(docId),
			Headers: map[string]string{
				"Authorization": auth,
				"If-Match":      strconv.FormatUint(casUint, 16), // Include CAS value
			},
		})
	}

	sendLockRequest := func(docId string, auth string) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   createLockPath(docId),
			Headers: map[string]string{
				"Authorization": auth,
			},
			Body: TEST_LOCK_CONTENT,
		})
	}

	s.Run("Basic", func() {
		// Lock the document first
		lockResp := sendLockRequest(docId, s.basicRestCreds)
		requireRestSuccess(s.T(), lockResp)
		assertRestValidEtag(s.T(), lockResp)

		casUint, err := strconv.ParseUint(lockResp.Headers.Get("ETag"), 16, 64)
		if err != nil {
			s.T().Fatalf("Failed to parse CAS value: %v", err)
		}

		resp := sendUnlockRequest(docId, s.basicRestCreds, casUint)
		requireRestSuccess(s.T(), resp)
	})

	s.Run("BadRequest", func() {
		docId, casInt := s.testDocIdAndCas()
		resp := sendUnlockRequest(docId, s.basicRestCreds, casInt)
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "DocumentNotLocked",
		})
	})

	s.Run("Forbidden", func() {
		docId := s.testDocId()
		// lock the document first
		lockResp := sendLockRequest(docId, s.basicRestCreds)
		requireRestSuccess(s.T(), lockResp)
		assertRestValidEtag(s.T(), lockResp)

		casUint, err := strconv.ParseUint(lockResp.Headers.Get("ETag"), 16, 64)
		if err != nil {
			s.T().Fatalf("Failed to parse CAS value: %v", err)
		}

		resp := sendUnlockRequest(docId, s.badRestCreds, casUint)
		requireRestError(s.T(), resp, http.StatusForbidden, &testRestError{
			Code: "InvalidAuth",
		})
	})

	s.Run("NotFound", func() {
		docId := "missing-unlocked-document"
		casUint := uint64(1)

		// No need to fetch CAS for a non-existent document
		resp := sendUnlockRequest(docId, s.basicRestCreds, casUint)
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	s.Run("Conflict", func() {
		docId, casInt := s.testDocIdAndCas()
		// lock the document first
		lockResp := sendLockRequest(docId, s.basicRestCreds)
		requireRestSuccess(s.T(), lockResp)
		assertRestValidEtag(s.T(), lockResp)

		resp := sendUnlockRequest(docId, s.basicRestCreds, casInt)
		requireRestError(s.T(), resp, http.StatusConflict, &testRestError{
			Code: "CasMismatch",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	// Test common DAPI error cases
	s.RunCommonDapiErrorCases(func(opts *commonDapiErrorTestData) *testHttpResponse {
		cas := uint64(1)
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/unlock",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: map[string]string{
				"Authorization": opts.Headers["Authorization"],
				"If-Match":      strconv.FormatUint(cas, 16),
			},
		})
	})
}

func (s *GatewayOpsTestSuite) TestTouchDocument() {
	// Helper function to create the request path
	createTouchPath := func(docId string) string {
		return fmt.Sprintf(
			"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
			s.bucketName, s.scopeName, s.collectionName, docId,
		)
	}

	// Helper function to send a request and return the response
	sendTouchRequest := func(docId string, auth string, body []byte) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   createTouchPath(docId),
			Headers: map[string]string{
				"Authorization": auth,
			},
			Body: body,
		})
	}

	s.Run("Basic", func() {
		docId := s.testDocId()

		resp := sendTouchRequest(docId, s.basicRestCreds, TEST_EXPIRY_CONTENT)
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.Contains(s.T(), string(resp.Body), string(TEST_CONTENT))
	})

	s.Run("BadRequest", func() {
		docId := s.testDocId()
		resp := sendTouchRequest(docId, s.basicRestCreds, []byte(`{"expiryTime": 100, "returnContent": true}`))
		requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
			Code: "InvalidExpiryFormat",
		})
	})

	s.Run("Forbidden", func() {
		docId := s.testDocId()

		resp := sendTouchRequest(docId, s.badRestCreds, TEST_EXPIRY_CONTENT)
		requireRestError(s.T(), resp, http.StatusForbidden, &testRestError{
			Code: "InvalidAuth",
		})
	})

	s.Run("NotFound", func() {
		docId := "missing-touch-document"

		resp := sendTouchRequest(docId, s.basicRestCreds, TEST_EXPIRY_CONTENT)
		requireRestError(s.T(), resp, http.StatusNotFound, &testRestError{
			Code: "DocumentNotFound",
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId),
		})
	})

	// Test common DAPI error cases
	s.RunCommonDapiErrorCases(func(opts *commonDapiErrorTestData) *testHttpResponse {
		return s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/touch",
				opts.BucketName, opts.ScopeName, opts.CollectionName, opts.DocumentKey,
			),
			Headers: map[string]string{
				"Authorization": opts.Headers["Authorization"],
			},
			Body: TEST_EXPIRY_CONTENT,
		})
	})
}
