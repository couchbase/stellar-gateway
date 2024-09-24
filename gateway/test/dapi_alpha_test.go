package test

import (
	"net/http"
)

func (s *GatewayOpsTestSuite) TestDapiAlphaEnabled() {
	resp := s.sendTestHttpRequest(&testHttpRequest{
		Method: http.MethodGet,
		Path:   "/v1.alpha/enabled",
	})
	requireRestSuccess(s.T(), resp)
}
