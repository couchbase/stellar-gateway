package test

import (
	"encoding/json"
	"net/http"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type QueryError struct {
	Code    int    `json:"code,omitempty"`
	Message string `json:"msg,omitempty"`
}

type QueryResponse struct {
	Results []map[string]interface{} `json:"results"`
	Errors  []QueryError             `json:"errors,omitempty"`
}

func (s *GatewayOpsTestSuite) TestDapiQueryProxy() {
	if !s.SupportsFeature(TestFeatureQuery) {
		s.T().Skip()
	}

	s.Run("Basic", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   "/_p/query/query/service",
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"statement": "SELECT 1 == 1"}`),
		})

		requireRestSuccess(s.T(), resp)

		queryResponse := QueryResponse{}

		err := json.Unmarshal(resp.Body, &queryResponse)
		require.NoError(s.T(), err)

		assert.Equal(s.T(), 1, len(queryResponse.Results))
		assert.Equal(s.T(), map[string]interface{}{"$1": true}, queryResponse.Results[0])
	})

	s.Run("Insert", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   "/_p/query/query/service",
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"statement": "UPSERT INTO default (KEY, VALUE) VALUES ('query-insert', { 'hello': 'world' })"}`),
		})

		requireRestSuccess(s.T(), resp)

		queryResponse := QueryResponse{}

		err := json.Unmarshal(resp.Body, &queryResponse)
		require.NoError(s.T(), err)
	})

	s.Run("Chunked", func() {
		// Send a query with lots of data to ensure chunking is used
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   "/_p/query/query/service",
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"statement": "SELECT i FROM ARRAY_RANGE(0,10000) AS i"}`),
		})

		requireRestSuccess(s.T(), resp)
		require.Equal(s.T(), []string{"chunked"}, resp.TransferEncoding)
		require.Empty(s.T(), resp.Headers.Get("Content-Length"))
	})

	// Query only performs authentication on the moment a request is
	// received from 7.2.8 onwards. Before this the request will be accepted
	// and a 200 returned.
	supportsNon200InvalidAuthResponse := !s.IsOlderServerVersion("7.2.8")

	s.Run("Unauthenticated", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   "/_p/query/query/service",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: []byte(`{"statement": "SELECT * FROM default LIMIT 1"}`),
		})

		if !supportsNon200InvalidAuthResponse {
			requireRestSuccess(s.T(), resp)

			queryResponse := QueryResponse{}
			err := json.Unmarshal(resp.Body, &queryResponse)
			require.NoError(s.T(), err)

			require.Equal(s.T(), 1, len(queryResponse.Errors))
			require.NotNil(s.T(), queryResponse.Errors[0])
			require.Equal(s.T(), 13014, queryResponse.Errors[0].Code)
			require.Contains(s.T(), queryResponse.Errors[0].Message, "User does not have credentials")
		} else {
			require.NotNil(s.T(), resp)
			require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
		}
	})

	s.Run("BadCredentials", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   "/_p/query/query/service",
			Headers: map[string]string{
				"Authorization": s.badRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"statement": "SELECT * FROM default LIMIT 1"}`),
		})

		// Query only performs authentication on the moment a request is
		// received from 7.2.8 onwards. Before this the request will be accepted
		// and a 200 returned.
		if !supportsNon200InvalidAuthResponse {
			requireRestSuccess(s.T(), resp)

			queryResponse := QueryResponse{}
			err := json.Unmarshal(resp.Body, &queryResponse)
			require.NoError(s.T(), err)

			require.Equal(s.T(), 1, len(queryResponse.Errors))
			require.NotNil(s.T(), queryResponse.Errors[0])
			require.Equal(s.T(), 13014, queryResponse.Errors[0].Code)
			require.Contains(s.T(), queryResponse.Errors[0].Message, "User does not have credentials")
		} else {
			require.NotNil(s.T(), resp)
			require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
		}
	})
}

func (s *GatewayOpsTestSuite) TestDapiAnalyticsProxy() {
	if !s.SupportsFeature(TestFeatureAnalytics) {
		s.T().Skip()
	}
	s.Run("Basic", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   "/_p/cbas/analytics/service",
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"statement": "SELECT 1 = 1"}`),
		})

		requireRestSuccess(s.T(), resp)

		queryResponse := QueryResponse{}

		err := json.Unmarshal(resp.Body, &queryResponse)
		require.NoError(s.T(), err)

		assert.Equal(s.T(), 1, len(queryResponse.Results))
		assert.Equal(s.T(), map[string]interface{}{"$1": true}, queryResponse.Results[0])
	})

	s.Run("Unauthenticated", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   "/_p/cbas/analytics/service",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: []byte(`{"statement": "SELECT 1 = 1"}`),
		})

		require.NotNil(s.T(), resp)
		require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
	})

	s.Run("BadCredentials", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   "/_p/cbas/analytics/service",
			Headers: map[string]string{
				"Authorization": s.badRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"statement": "SELECT 1 = 1"}`),
		})

		require.NotNil(s.T(), resp)
		require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
	})
}

func (s *GatewayOpsTestSuite) TestDapiManagementProxy() {
	s.Run("GetBuckets", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path:   "/_p/mgmt/pools/default/buckets",
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
		})

		requireRestSuccess(s.T(), resp)

		type Bucket struct {
			Name string `json:"name"`
		}

		var bucketsResponse []Bucket

		err := json.Unmarshal(resp.Body, &bucketsResponse)
		require.NoError(s.T(), err)

		assert.Equal(s.T(), 1, len(bucketsResponse))
		assert.Equal(s.T(), "default", bucketsResponse[0].Name)
	})

	s.Run("Unauthenticated", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path:   "/_p/mgmt/pools/default/buckets",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
		})

		require.NotNil(s.T(), resp)
		require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
	})

	s.Run("BadCredentials", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path:   "/_p/mgmt/pools/default/buckets",
			Headers: map[string]string{
				"Authorization": s.badRestCreds,
				"Content-Type":  "application/json",
			},
		})

		require.NotNil(s.T(), resp)
		require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
	})
}

func (s *GatewayOpsTestSuite) TestDapiSearchProxy() {
	if !s.SupportsFeature(TestFeatureSearch) {
		s.T().Skip()
	}

	s.Run("GetIndexes", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path:   "/_p/fts/api/index",
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
				"Content-Type":  "application/json",
			},
		})

		requireRestSuccess(s.T(), resp)

		type IndexResponse struct {
			Status  string      `json:"status"`
			Indexes interface{} `json:"indexDefs"`
		}

		indexResponse := IndexResponse{}
		err := json.Unmarshal(resp.Body, &indexResponse)
		require.NoError(s.T(), err)

		assert.Equal(s.T(), "ok", indexResponse.Status)
	})

	s.Run("Unauthenticated", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path:   "/_p/fts/api/index",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
		})

		require.NotNil(s.T(), resp)
		require.Equal(s.T(), http.StatusForbidden, resp.StatusCode)
	})

	s.Run("BadCredentials", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path:   "/_p/fts/api/index",
			Headers: map[string]string{
				"Authorization": s.badRestCreds,
				"Content-Type":  "application/json",
			},
		})

		require.NotNil(s.T(), resp)
		require.Equal(s.T(), http.StatusForbidden, resp.StatusCode)
	})
}
