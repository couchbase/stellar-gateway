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
	StatusCode       int
	Headers          http.Header
	TransferEncoding []string
	Body             []byte
}

func (s *GatewayOpsTestSuite) sendTestHttpRequest(req *testHttpRequest) *testHttpResponse {
	fmt.Println("JW SENDING REQUEST")
	hreq, err := http.NewRequest(
		req.Method,
		fmt.Sprintf("https://%s%s", s.dapiAddr, req.Path),
		bytes.NewReader(req.Body))
	require.NoError(s.T(), err)

	for k, v := range req.Headers {
		hreq.Header.Set(k, v)
	}

	if hreq.Header.Get("User-Agent") == "" {
		hreq.Header.Set("User-Agent", "dapi-test")
	}

	fmt.Println("JW SENDING REQUEST 2")

	hresp, err := s.dapiCli.Do(hreq)
	require.NoError(s.T(), err)

	fmt.Println("JW SENDING REQUEST 3")

	fullBody, err := io.ReadAll(hresp.Body)
	require.NoError(s.T(), err)

	_ = hresp.Body.Close()

	fmt.Println("JW SENDING REQUEST 4")

	return &testHttpResponse{
		StatusCode:       hresp.StatusCode,
		Headers:          hresp.Header,
		TransferEncoding: hresp.TransferEncoding,
		Body:             fullBody,
	}
}
