package test

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *GatewayOpsTestSuite) TestDapiClientCertAuth() {
	testutils.SkipIfNoDinoCluster(s.T())

	s.Run("Tools", s.Tools)

	s.Run("Proxy", s.Proxy)

	s.Run("Crud", s.Crud)
}

func (s *GatewayOpsTestSuite) Tools() {
	dino := testutils.StartDinoTesting(s.T(), false)
	username := "dapiUser"
	client := s.createMtlsClient(dino, username)

	s.Run("NonExistentUser", func() {
		resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
			Method: http.MethodGet,
			Path:   "/v1/callerIdentity",
		}, client)
		require.Equal(s.T(), http.StatusForbidden, resp.StatusCode)
		require.Contains(s.T(), string(resp.Body), "Your certificate is invalid")

	})

	dino.AddUnprivilegedUser(username)
	s.T().Cleanup(func() {
		dino.RemoveUser(username)
	})

	s.Run("Success", func() {
		require.Eventually(s.T(), func() bool {
			resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
				Method: http.MethodGet,
				Path:   "/v1/callerIdentity",
			}, client)
			if resp.StatusCode != http.StatusOK {
				return false
			}

			return assert.JSONEq(s.T(), fmt.Sprintf(`{"user":"%s"}`, username), string(resp.Body))
		}, time.Second*30, time.Second)
	})
}

func (s *GatewayOpsTestSuite) Proxy() {
	if !s.SupportsFeature(TestFeatureQuery) {
		s.T().Skip()
	}

	dino := testutils.StartDinoTesting(s.T(), false)
	username := "proxyUser"
	client := s.createMtlsClient(dino, username)

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

	dino.AddUnprivilegedUser(username)
	s.T().Cleanup(func() {
		dino.RemoveUser(username)
	})

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

	dino.AddWriteUser(username)

	s.Run("Success", func() {
		require.Eventually(s.T(), func() bool {
			resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
				Method: http.MethodPost,
				Path:   "/_p/query/query/service",
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
				Body: []byte(`{"statement": "UPSERT INTO default (KEY, VALUE) VALUES ('query-insert', { 'hello': 'world' })"}`),
			}, client)
			return resp != nil && resp.StatusCode == http.StatusOK
		}, time.Second*30, time.Second)
	})
}

func (s *GatewayOpsTestSuite) Crud() {
	dino := testutils.StartDinoTesting(s.T(), false)
	username := "crudUser"
	client := s.createMtlsClient(dino, username)

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

	dino.AddUnprivilegedUser(username)
	s.T().Cleanup(func() {
		dino.RemoveUser(username)
	})

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

	dino.AddReadOnlyUser(username)

	s.Run("ReadSuccess", func() {
		require.Eventually(s.T(), func() bool {
			resp := s.sendTestHttpRequestWithClient(&testHttpRequest{
				Method: http.MethodGet,
				Path: fmt.Sprintf(
					"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
					s.bucketName, s.scopeName, s.collectionName, s.testDocId(),
				),
			}, client)
			return resp != nil && resp.StatusCode == http.StatusOK
		}, time.Second*30, time.Second)
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

	dino.AddWriteUser(username)

	s.Run("WriteSuccess", func() {
		require.Eventually(s.T(), func() bool {
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
			return resp != nil && resp.StatusCode == http.StatusOK
		}, time.Second*30, time.Second)
	})
}

func (s *GatewayOpsTestSuite) createMtlsClient(dino *testutils.DinoController, username string) *http.Client {
	res := dino.GetClientCert(username)

	cert, err := tls.X509KeyPair([]byte(res), []byte(res))
	assert.NoError(s.T(), err)

	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:      s.clientCaCertPool,
				Certificates: []tls.Certificate{cert},
			},
		},
	}
}
