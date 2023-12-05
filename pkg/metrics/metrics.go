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

	return &SnMetrics{
		NewConnections:    newConnections,
		ActiveConnections: activeConnections,
	}
}
