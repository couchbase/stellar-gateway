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
