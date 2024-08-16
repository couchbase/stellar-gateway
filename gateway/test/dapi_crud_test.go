package test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testHttpRequest struct {
	Method  string
	Path    string
	Headers map[string]string
	Body    []byte
}

type testHttpResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

func (s *GatewayOpsTestSuite) sendTestHttpRequest(req *testHttpRequest) *testHttpResponse {
	hreq, err := http.NewRequest(
		req.Method,
		fmt.Sprintf("http://%s%s", s.dapiAddr, req.Path),
		bytes.NewReader(req.Body))
	require.NoError(s.T(), err)

	for k, v := range req.Headers {
		hreq.Header.Set(k, v)
	}

	hresp, err := http.DefaultClient.Do(hreq)
	require.NoError(s.T(), err)

	fullBody, err := io.ReadAll(hresp.Body)
	require.NoError(s.T(), err)

	hresp.Body.Close()

	return &testHttpResponse{
		StatusCode: hresp.StatusCode,
		Headers:    hresp.Header,
		Body:       fullBody,
	}
}

func (s *GatewayOpsTestSuite) TestDapiCors() {
	docId := s.randomDocId()

	resp := s.sendTestHttpRequest(&testHttpRequest{
		Method: http.MethodOptions,
		Path: fmt.Sprintf(
			"/v1/buckets/%s/scopes/%s/collections/%s/docs/%s",
			s.bucketName, s.scopeName, s.collectionName, docId,
		),
		Headers: map[string]string{
			"Authorization":                 s.basicRestCreds,
			"Access-Control-Request-Method": http.MethodPut,
			"Origin":                        "http://example.com",
		},
	})
	require.NotNil(s.T(), resp)
	require.Equal(s.T(), http.StatusNoContent, resp.StatusCode)
	assert.Equal(s.T(), "*",
		resp.Headers.Get("Access-Control-Allow-Origin"))

	resp = s.sendTestHttpRequest(&testHttpRequest{
		Method: http.MethodPut,
		Path: fmt.Sprintf(
			"/v1/buckets/%s/scopes/%s/collections/%s/docs/%s",
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
			"/v1/buckets/%s/scopes/%s/collections/%s/docs/%s",
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

func (s *GatewayOpsTestSuite) TestDapiBasic() {
	if !s.SupportsFeature(TestFeatureKV) {
		s.T().Skip()
	}

	docId := s.randomDocId()

	s.Run("Post", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/docs/%s",
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

	s.Run("Get", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/docs/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
			Body: TEST_CONTENT,
		})
		requireRestSuccess(s.T(), resp)
		assertRestValidEtag(s.T(), resp)
		assert.Equal(s.T(), fmt.Sprintf("%d", TEST_CONTENT_FLAGS), resp.Headers.Get("X-CB-Flags"))
		assert.Equal(s.T(), TEST_CONTENT, resp.Body)
		assert.Equal(s.T(), "", resp.Headers.Get("X-CB-Expiry"))
	})

	s.Run("Put", func() {
		newContent := []byte(`{"boo": "baz"}`)

		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPut,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/docs/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
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

	s.Run("Delete", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodDelete,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/docs/%s",
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
}
