/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

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
		fmt.Sprintf("https://%s%s", s.dapiAddr, req.Path),
		bytes.NewReader(req.Body))
	require.NoError(s.T(), err)

	for k, v := range req.Headers {
		hreq.Header.Set(k, v)
	}

	if hreq.Header.Get("User-Agent") == "" {
		hreq.Header.Set("User-Agent", "dapi-test")
	}

	hresp, err := s.dapiCli.Do(hreq)
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
