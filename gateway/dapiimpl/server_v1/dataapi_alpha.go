/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

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
