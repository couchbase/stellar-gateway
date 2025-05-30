package test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireRestSuccess(t *testing.T, resp *testHttpResponse) {
	require.NotNil(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode, fmt.Sprintf("status code was not 200 body::\n%s", string(resp.Body)))
}

func requireRestSuccessNoContent(t *testing.T, resp *testHttpResponse) {
	require.NotNil(t, resp)
	require.Equal(t, http.StatusNoContent, resp.StatusCode, fmt.Sprintf("status code was not 202 body::\n%s", string(resp.Body)))
}

type testRestError struct {
	Code     string
	Resource string
}

type restErrorJson struct {
	Code     string `json:"code,omitempty"`
	Resource string `json:"resource,omitempty"`
}

func requireRestError(t *testing.T, resp *testHttpResponse, expectedCode int, expectedError *testRestError) {
	require.NotNil(t, resp)

	require.Equal(t, expectedCode, resp.StatusCode,
		fmt.Sprintf("Unexpected status code %d, expected %d, body: %s",
			resp.StatusCode, expectedCode, resp.Body))

	var restErr restErrorJson
	err := json.Unmarshal(resp.Body, &restErr)
	require.NoError(t, err)

	if expectedError != nil {
		if expectedError.Code != "" {
			require.Equal(t, expectedError.Code, restErr.Code)
		}
		if expectedError.Resource != "" {
			require.Equal(t, expectedError.Resource, restErr.Resource)
		}
	}
}

func assertRestValidEtag(t *testing.T, resp *testHttpResponse) {
	etag := resp.Headers.Get("Etag")
	assert.NotEmpty(t, etag)
}

func assertRestValidMutationToken(t *testing.T, resp *testHttpResponse, bucketName string) {
	token := resp.Headers.Get("X-CB-MutationToken")
	if assert.NotEmpty(t, token) {

		// fmt.Sprintf("%s:%d:%08x:%d", bucketName, token.VbID, token.VbUuid, token.SeqNo)
		parts := strings.Split(token, ":")
		assert.Len(t, parts, 4)

		if len(parts) >= 4 {
			assert.NotEmpty(t, parts[0])
			assert.Equal(t, bucketName, parts[0])

			assert.NotEmpty(t, parts[1])

			assert.NotEmpty(t, parts[2])

			assert.NotEmpty(t, parts[3])
		}
	}
}
