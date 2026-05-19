package dapiimpl

import (
	"context"
	"net/http"

	"github.com/couchbase/gocbcorex/contrib/buildversion"
	"github.com/couchbase/stellar-gateway/dataapiv1"
	"github.com/couchbase/stellar-gateway/utils/cbclientnames"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	buildVersion = buildversion.GetVersion("github.com/couchbase/stellar-gateway")
	meter        = otel.Meter("github.com/couchbase/stellar-gateway/gateway/dapiimpl",
		metric.WithInstrumentationVersion(buildVersion))
)

func NewUserAgentMetricsHandler() dataapiv1.StrictMiddlewareFunc {
	clientNames, _ := meter.Int64Counter("dataapi_client_request_count")

	return func(f dataapiv1.StrictHandlerFunc, operationID string) dataapiv1.StrictHandlerFunc {
		return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request any) (any, error) {
			userAgent := r.Header.Get("user-agent")
			clientName := cbclientnames.FromUserAgent(userAgent)

			if clientName != "" {
				clientNames.Add(ctx, 1,
					metric.WithAttributes(attribute.String("client_name", clientName)))
			}

			return f(ctx, w, r, request)
		}
	}
}
