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

func (s *DataApiServer) GetCallerIdentity(
	ctx context.Context, in dataapiv1.GetCallerIdentityRequestObject,
) (dataapiv1.GetCallerIdentityResponseObject, error) {
	user, _, errSt := s.authHandler.GetOboUserFromRequest(ctx, in.Params.Authorization)
	if errSt != nil {
		return nil, errSt.Err()
	}

	return dataapiv1.GetCallerIdentity200JSONResponse{
		User: &user,
	}, nil
}
