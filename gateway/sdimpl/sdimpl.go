/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package sdimpl

import (
	"github.com/couchbase/stellar-gateway/gateway/sdimpl/server_v1"
	"github.com/couchbase/stellar-gateway/gateway/topology"
	"go.uber.org/zap"
)

type NewOptions struct {
	Logger *zap.Logger

	TopologyProvider topology.Provider
}

type Servers struct {
	RoutingV1Server *server_v1.RoutingServer
}

func New(opts *NewOptions) *Servers {
	return &Servers{
		RoutingV1Server: server_v1.NewRoutingServer(opts.TopologyProvider, opts.Logger.Named("routing-server")),
	}
}
