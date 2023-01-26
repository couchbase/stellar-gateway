package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type SnMetrics struct {
	NewConnections    prometheus.Counter
	ActiveConnections prometheus.Gauge
}

var (
	snMetrics *SnMetrics
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

func newSnMetrics() *SnMetrics {
	return &SnMetrics{
		NewConnections: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "sn",
			Name: "grpc_new_connections",
			Help: "The number of new gRPC connections that have been accepted.",
		}),
		ActiveConnections: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "sn",
			Name: "grpc_active_connections",
			Help: "The number of active grpc connections.",
		}),
	}
}