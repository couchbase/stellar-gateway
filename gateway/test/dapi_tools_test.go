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
	"fmt"
	"net/http"

	"github.com/stretchr/testify/assert"
)

func (s *GatewayOpsTestSuite) TestDapiCallerIdentity() {
	s.Run("Basic", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path:   "/v1/callerIdentity",
			Headers: map[string]string{
				"Authorization": s.basicRestCreds,
			},
		})
		requireRestSuccess(s.T(), resp)
		assert.JSONEq(s.T(), fmt.Sprintf(`{"user":"%s"}`, s.testClusterInfo.BasicUser), string(resp.Body))
	})

	s.Run("InvalidAuth", func() {
		resp := s.sendTestHttpRequest(&testHttpRequest{
			Method: http.MethodGet,
			Path:   "/v1/callerIdentity",
			Headers: map[string]string{
				"Authorization": s.badRestCreds,
			},
		})
		requireRestError(s.T(), resp, http.StatusForbidden, &testRestError{
			Code: "InvalidAuth",
		})
	})
}
