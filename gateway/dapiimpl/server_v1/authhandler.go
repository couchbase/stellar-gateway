package server_v1

import (
	"context"
	"errors"
	"net/http"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/stellar-gateway/gateway/auth"
	"go.uber.org/zap"
)

type AuthHandler struct {
	Logger        *zap.Logger
	ErrorHandler  *ErrorHandler
	Authenticator auth.Authenticator
	CbClient      *gocbcorex.BucketsTrackingAgentManager
}

func (a AuthHandler) getUserPassFromRequest(authHdr string) (string, string, error) {
	// we reuse the Basic auth parsing built into the go net library
	r := http.Request{
		Header: map[string][]string{
			"Authorization": {authHdr},
		},
	}
	username, password, ok := r.BasicAuth()
	if !ok {
		a.Logger.Debug("failed to parse authorization header")
		return "", "", errors.New("failed to parse authorization header")
	}

	return username, password, nil
}

func (a AuthHandler) MaybeGetUserPassFromRequest(authHdr string) (string, string, *Status) {
	username, password, err := a.getUserPassFromRequest(authHdr)
	if err != nil {
		return "", "", a.ErrorHandler.NewInvalidAuthHeaderStatus(err)
	}

	return username, password, nil
}

func (a AuthHandler) MaybeGetOboUserFromContext(ctx context.Context, authHdr string) (string, string, *Status) {
	username, password, errHe := a.MaybeGetUserPassFromRequest(authHdr)
	if errHe != nil {
		return "", "", errHe
	}

	if username == "" && password == "" {
		return "", "", nil
	}

	oboUser, oboDomain, err := a.Authenticator.ValidateUserForObo(ctx, username, password)
	if err != nil {
		if errors.Is(err, auth.ErrInvalidCredentials) {
			return "", "", a.ErrorHandler.NewInvalidCredentialsStatus()
		}

		a.Logger.Error("received an unexpected authentication error", zap.Error(err))
		return "", "", a.ErrorHandler.NewInternalStatus()
	}

	return oboUser, oboDomain, nil
}

func (a AuthHandler) GetOboUserFromRequest(ctx context.Context, authHdr string) (string, string, *Status) {
	user, domain, st := a.MaybeGetOboUserFromContext(ctx, authHdr)
	if st != nil {
		return "", "", st
	}

	if user == "" {
		return "", "", a.ErrorHandler.NewNoAuthStatus()
	}

	return user, domain, nil
}

func (a AuthHandler) GetHttpOboInfoFromContext(ctx context.Context, authHdr string) (*cbhttpx.OnBehalfOfInfo, *Status) {
	username, password, errHe := a.MaybeGetUserPassFromRequest(authHdr)
	if errHe != nil {
		return nil, errHe
	}

	if username == "" {
		return nil, a.ErrorHandler.NewNoAuthStatus()
	}

	return &cbhttpx.OnBehalfOfInfo{
		Username: username,
		Password: password,
	}, nil
}

func (a AuthHandler) getClusterAgent(ctx context.Context) (*gocbcorex.Agent, *Status) {
	agent, err := a.CbClient.GetClusterAgent(ctx)
	if err != nil {
		return nil, a.ErrorHandler.NewGenericStatus(err)
	}

	return agent, nil
}

func (a AuthHandler) getBucketAgent(ctx context.Context, bucketName string) (*gocbcorex.Agent, *Status) {
	bucketAgent, err := a.CbClient.GetBucketAgent(ctx, bucketName)
	if err != nil {
		if errors.Is(err, cbmgmtx.ErrBucketNotFound) {
			return nil, a.ErrorHandler.NewBucketMissingStatus(err, bucketName)
		}

		return nil, a.ErrorHandler.NewGenericStatus(err)
	}

	return bucketAgent, nil
}

func (a AuthHandler) GetMemdOboAgent(
	ctx context.Context, authHdr string, bucketName string,
) (*gocbcorex.Agent, string, *Status) {
	oboUser, _, errHe := a.GetOboUserFromRequest(ctx, authHdr)
	if errHe != nil {
		return nil, "", errHe
	}

	bucketAgent, errHe := a.getBucketAgent(ctx, bucketName)
	if errHe != nil {
		return nil, "", errHe
	}

	return bucketAgent, oboUser, nil
}

func (a AuthHandler) GetHttpOboAgent(
	ctx context.Context, authHdr string, bucketName *string,
) (*gocbcorex.Agent, *cbhttpx.OnBehalfOfInfo, *Status) {
	oboInfo, errHe := a.GetHttpOboInfoFromContext(ctx, authHdr)
	if errHe != nil {
		return nil, nil, errHe
	}

	var agent *gocbcorex.Agent
	if bucketName == nil {
		clusterAgent, errHe := a.getClusterAgent(ctx)
		if errHe != nil {
			return nil, nil, errHe
		}

		agent = clusterAgent
	} else {
		bucketAgent, errHe := a.getBucketAgent(ctx, *bucketName)
		if errHe != nil {
			return nil, nil, errHe
		}

		agent = bucketAgent
	}

	return agent, oboInfo, nil
}
