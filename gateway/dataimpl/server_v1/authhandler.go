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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type AuthHandler struct {
	Logger        *zap.Logger
	ErrorHandler  *ErrorHandler
	Authenticator auth.Authenticator
	CbClient      *gocbcorex.AgentManager
}

func (a AuthHandler) getUserPassFromMetaData(md metadata.MD) (string, string, error) {
	authValues := md.Get("authorization")
	if len(authValues) > 1 {
		a.Logger.Debug("more than a single authorization header was found")
		return "", "", errors.New("more than a single authorization header was found")
	}

	if len(authValues) == 0 {
		return "", "", nil
	}

	// we reuse the Basic auth parsing built into the go net library
	r := http.Request{
		Header: map[string][]string{
			"Authorization": authValues,
		},
	}
	username, password, ok := r.BasicAuth()
	if !ok {
		a.Logger.Debug("failed to parse authorization header")
		return "", "", errors.New("failed to parse authorization header")
	}

	return username, password, nil
}

func (a AuthHandler) MaybeGetUserPassFromContext(ctx context.Context) (string, string, *status.Status) {
	md, hasMd := metadata.FromIncomingContext(ctx)
	if !hasMd {
		a.Logger.Error("failed to fetch grpc metadata from context")
		return "", "", a.ErrorHandler.NewInternalStatus()
	}

	username, password, err := a.getUserPassFromMetaData(md)
	if err != nil {
		return "", "", a.ErrorHandler.NewInvalidAuthHeaderStatus(err)
	}

	return username, password, nil
}

func (a AuthHandler) MaybeGetOboUserFromContext(ctx context.Context) (string, string, *status.Status) {
	username, password, errSt := a.MaybeGetUserPassFromContext(ctx)
	if errSt != nil {
		return "", "", errSt
	}

	if username == "" && password == "" {
		return "", "", nil
	}

	oboUser, oboDomain, err := a.Authenticator.ValidateUserForObo(username, password)
	if err != nil {
		if errors.Is(err, auth.ErrInvalidCredentials) {
			return "", "", a.ErrorHandler.NewInvalidCredentialsStatus()
		}

		a.Logger.Error("received an unexpected authentication error", zap.Error(err))
		return "", "", a.ErrorHandler.NewInternalStatus()
	}

	return oboUser, oboDomain, nil
}

func (a AuthHandler) GetOboUserFromContext(ctx context.Context) (string, string, *status.Status) {
	user, domain, st := a.MaybeGetOboUserFromContext(ctx)
	if st != nil {
		return "", "", st
	}

	if user == "" {
		return "", "", a.ErrorHandler.NewNoAuthStatus()
	}

	return user, domain, nil
}

func (a AuthHandler) GetHttpOboInfoFromContext(ctx context.Context) (*cbhttpx.OnBehalfOfInfo, *status.Status) {
	username, password, errSt := a.MaybeGetUserPassFromContext(ctx)
	if errSt != nil {
		return nil, errSt
	}

	if username == "" {
		return nil, a.ErrorHandler.NewNoAuthStatus()
	}

	return &cbhttpx.OnBehalfOfInfo{
		Username: username,
		Password: password,
	}, nil
}

func (a AuthHandler) getClusterAgent(ctx context.Context) (*gocbcorex.Agent, *status.Status) {
	agent, err := a.CbClient.GetClusterAgent()
	if err != nil {
		return nil, a.ErrorHandler.NewGenericStatus(err)
	}

	return agent, nil
}

func (a AuthHandler) getBucketAgent(ctx context.Context, bucketName string) (*gocbcorex.Agent, *status.Status) {
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
	ctx context.Context, bucketName string,
) (*gocbcorex.Agent, string, *status.Status) {
	oboUser, _, errSt := a.GetOboUserFromContext(ctx)
	if errSt != nil {
		return nil, "", errSt
	}

	bucketAgent, errSt := a.getBucketAgent(ctx, bucketName)
	if errSt != nil {
		return nil, "", errSt
	}

	return bucketAgent, oboUser, nil
}

func (a AuthHandler) GetHttpOboAgent(
	ctx context.Context, bucketName *string,
) (*gocbcorex.Agent, *cbhttpx.OnBehalfOfInfo, *status.Status) {
	oboInfo, errSt := a.GetHttpOboInfoFromContext(ctx)
	if errSt != nil {
		return nil, nil, errSt
	}

	var agent *gocbcorex.Agent
	if bucketName == nil {
		clusterAgent, errSt := a.getClusterAgent(ctx)
		if errSt != nil {
			return nil, nil, errSt
		}

		agent = clusterAgent
	} else {
		bucketAgent, errSt := a.getBucketAgent(ctx, *bucketName)
		if errSt != nil {
			return nil, nil, errSt
		}

		agent = bucketAgent
	}

	return agent, oboInfo, nil
}
