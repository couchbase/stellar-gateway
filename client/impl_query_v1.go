/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package client

import (
	"context"

	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"google.golang.org/grpc"
)

type routingImpl_QueryV1 struct {
	client *RoutingClient
}

// Verify that RoutingClient implements Conn
var _ query_v1.QueryServiceClient = (*routingImpl_QueryV1)(nil)

func (c *routingImpl_QueryV1) Query(ctx context.Context, in *query_v1.QueryRequest, opts ...grpc.CallOption) (query_v1.QueryService_QueryClient, error) {
	if in.BucketName != nil {
		return c.client.fetchConnForBucket(*in.BucketName).QueryV1().Query(ctx, in, opts...)
	} else {
		return c.client.fetchConn().QueryV1().Query(ctx, in, opts...)
	}
}
