package server_v1

import (
	"context"

	"github.com/couchbase/stellar-gateway/dataapiv1"
)

func (s *DataApiServer) AlphaEnabled(
	ctx context.Context, in dataapiv1.AlphaEnabledRequestObject,
) (dataapiv1.AlphaEnabledResponseObject, error) {
	return dataapiv1.AlphaEnabled200Response{}, nil
}
