package test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

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
