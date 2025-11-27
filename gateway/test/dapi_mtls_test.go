package test

import (
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *GatewayOpsTestSuite) TestDapiClientCertAuthTools() {
	testutils.SkipIfNoDinoCluster(s.T())
	client := s.clientFromCert(s.missingUserCert)

	s.Run("NonExistentUser", func() {
		resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
			Method: http.MethodGet,
			Path:   "/v1/callerIdentity",
		}, client)
		require.Equal(s.T(), http.StatusForbidden, resp.StatusCode)
		require.Contains(s.T(), string(resp.Body), "Your certificate is invalid")

	})

	client = s.clientFromCert(s.noPermsCert)

	s.Run("Success", func() {
		resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
			Method: http.MethodGet,
			Path:   "/v1/callerIdentity",
		}, client)

		assert.JSONEq(s.T(), `{"user":"no-permissions"}`, string(resp.Body))
	})
}

func (s *GatewayOpsTestSuite) TestDapiClientCertAuthProxy() {
	testutils.SkipIfNoDinoCluster(s.T())
	if !s.SupportsFeature(TestFeatureQuery) {
		s.T().Skip()
	}

	client := s.clientFromCert(s.missingUserCert)

	s.Run("NonExistentUser", func() {
		resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
			Method: http.MethodPost,
			Path:   "/_p/query/query/service",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: []byte(`{"statement": "UPSERT INTO default (KEY, VALUE) VALUES ('query-insert', { 'hello': 'world' })"}`),
		}, client)
		require.Equal(s.T(), http.StatusBadGateway, resp.StatusCode)
		require.Contains(s.T(), string(resp.Body), "failed to validate certificate")
	})

	client = s.clientFromCert(s.noPermsCert)

	s.Run("InsufficientPermissions", func() {
		resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
			Method: http.MethodPost,
			Path:   "/_p/query/query/service",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: []byte(`{"statement": "UPSERT INTO default (KEY, VALUE) VALUES ('query-insert', { 'hello': 'world' })"}`),
		}, client)
		require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
		require.Contains(s.T(), string(resp.Body), "User does not have credentials to run INSERT queries")
	})

	client = s.clientFromCert(s.basicUserCert)

	s.Run("Success", func() {
		resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
			Method: http.MethodPost,
			Path:   "/_p/query/query/service",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: []byte(`{"statement": "UPSERT INTO default (KEY, VALUE) VALUES ('query-insert', { 'hello': 'world' })"}`),
		}, client)
		requireRestSuccess(s.T(), resp)
	})
}

func (s *GatewayOpsTestSuite) TestDapiClientCertAuthCrud() {
	testutils.SkipIfNoDinoCluster(s.T())
	client := s.clientFromCert(s.missingUserCert)

	s.Run("NonExistentUser", func() {
		resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
		}, client)
		require.Equal(s.T(), http.StatusForbidden, resp.StatusCode)
		require.Contains(s.T(), string(resp.Body), "Your certificate is invalid")
	})

	client = s.clientFromCert(s.noPermsCert)

	s.Run("InsufficientReadPermissions", func() {
		resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
		}, client)
		require.Equal(s.T(), http.StatusForbidden, resp.StatusCode)
		require.Contains(s.T(), string(resp.Body), "access error")
	})

	client = s.clientFromCert(s.readUserCert)

	s.Run("ReadSuccess", func() {
		resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
			Method: http.MethodGet,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
			),
		}, client)
		requireRestSuccess(s.T(), resp)
	})

	s.Run("InsufficientWritePermissions", func() {
		docId := s.randomDocId()
		resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"X-CB-Flags": fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
			},
			Body: TEST_CONTENT,
		}, client)
		require.Equal(s.T(), http.StatusForbidden, resp.StatusCode)
		require.Contains(s.T(), string(resp.Body), "access error")
	})

	client = s.clientFromCert(s.basicUserCert)

	s.Run("WriteSuccess", func() {
		docId := s.randomDocId()
		resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
			Method: http.MethodPost,
			Path: fmt.Sprintf(
				"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
				s.bucketName, s.scopeName, s.collectionName, docId,
			),
			Headers: map[string]string{
				"X-CB-Flags": fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
			},
			Body: TEST_CONTENT,
		}, client)
		requireRestSuccess(s.T(), resp)
	})
}

func (s *GatewayOpsTestSuite) clientFromCert(cert *tls.Certificate) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:      s.clientCaCertPool,
				Certificates: []tls.Certificate{*cert},
			},
		},
	}
}
