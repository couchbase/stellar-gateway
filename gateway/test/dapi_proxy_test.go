package test

import (
	"encoding/json"
	"net/http"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type QueryResponse struct {
	Results []map[string]interface{} `json:"results"`
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
		assert.Equal(s.T(), queryResponse.Results[0], map[string]interface{}{"$1": true})
	})

	s.Run("Unauthenticated", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   "/_p/query/query/service",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: []byte(`{"statement": "SELECT 1 == 1"}`),
		})

		require.NotNil(s.T(), resp)
		require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
	})

	s.Run("BadCredentials", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodPost,
			Path:   "/_p/query/query/service",
			Headers: map[string]string{
				"Authorization": s.badRestCreds,
				"Content-Type":  "application/json",
			},
			Body: []byte(`{"statement": "SELECT 1 == 1"}`),
		})

		require.NotNil(s.T(), resp)
		require.Equal(s.T(), http.StatusUnauthorized, resp.StatusCode)
	})
}

func (s *GatewayOpsTestSuite) TestDapiAnalyticsProxy() {
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
		assert.Equal(s.T(), queryResponse.Results[0], map[string]interface{}{"$1": true})
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
