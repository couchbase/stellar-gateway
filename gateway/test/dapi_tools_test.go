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
