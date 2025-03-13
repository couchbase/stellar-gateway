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
	"context"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func (s *GatewayOpsTestSuite) TestHealth() {
	healthClient := grpc_health_v1.NewHealthClient(s.gatewayConn)

	s.Run("General", func() {
		resp, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
			Service: "",
		})
		requireRpcSuccess(s.T(), resp, err)

		require.Equal(s.T(), grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)
	})

	s.Run("InvalidService", func() {
		_, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
			Service: "couchbase.invalid.v1",
		})
		assertRpcStatus(s.T(), err, codes.NotFound)
	})

	services := []string{
		"couchbase.kv.v1.KvService",
		"couchbase.query.v1.QueryService",
		"couchbase.search.v1.SearchService",
		"couchbase.admin.bucket.v1.BucketAdminService",
		"couchbase.admin.collection.v1.CollectionAdminService",
		"couchbase.admin.query.v1.QueryAdminService",
		"couchbase.admin.search.v1.SearchAdminService",
	}
	for _, serviceName := range services {
		s.Run(serviceName, func() {
			resp, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
				Service: serviceName,
			})
			requireRpcSuccess(s.T(), resp, err)

			require.Equal(s.T(), grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)
		})

	}
}
