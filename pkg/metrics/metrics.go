/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metrics

import (
	"sync"

	"github.com/couchbase/gocbcorex/contrib/buildversion"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type SnMetrics struct {
	NewConnections    metric.Float64Counter
	ActiveConnections metric.Float64UpDownCounter
	ClientNames       metric.Int64Counter
}

var (
	snMetrics     *SnMetrics
	snMetricsLock sync.Mutex
)

func GetSnMetrics() *SnMetrics {
	snMetricsLock.Lock()

	if snMetrics != nil {
		snMetricsLock.Unlock()
		return snMetrics
	}

	snMetrics = newSnMetrics()

	snMetricsLock.Unlock()
	return snMetrics
}

var buildVersion string = buildversion.GetVersion("github.com/couchbase/stellar-gateway")

func newSnMetrics() *SnMetrics {
	meter := otel.Meter(
		"com.couchbase.cloud-native-gateway",
		metric.WithInstrumentationVersion(buildVersion))

	newConnections, _ := meter.Float64Counter("grpc_connections_total")
	activeConnections, _ := meter.Float64UpDownCounter("grpc_connections")
	clientNames, _ := meter.Int64Counter("grpc_client_names")

	return &SnMetrics{
		NewConnections:    newConnections,
		ActiveConnections: activeConnections,
		ClientNames:       clientNames,
	}
}
