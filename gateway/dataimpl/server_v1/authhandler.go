package server_v1

import (
	"context"
	"errors"
	"net/http"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/stellar-gateway/gateway/auth"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type AuthHandler struct {
	Logger        *zap.Logger
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

func (a AuthHandler) MaybeGetOboUserFromContext(ctx context.Context) (string, *status.Status) {
	md, hasMd := metadata.FromIncomingContext(ctx)
	if !hasMd {
		a.Logger.Error("failed to fetch grpc metadata from context")
		return "", newInternalStatus()
	}

	username, password, err := a.getUserPassFromMetaData(md)
	if err != nil {
		return "", newInvalidAuthHeaderStatus(err)
	}

	if username == "" && password == "" {
		return "", nil
	}

	oboUser, err := a.Authenticator.ValidateUserForObo(username, password)
	if err != nil {
		if errors.Is(err, auth.ErrInvalidCredentials) {
			return "", newInvalidCredentialsStatus()
		}

		a.Logger.Error("received an unexpected authentication error", zap.Error(err))
		return "", newInternalStatus()
	}

	return oboUser, nil
}

func (a AuthHandler) GetOboUserFromContext(ctx context.Context) (string, *status.Status) {
	user, st := a.MaybeGetOboUserFromContext(ctx)
	if st != nil {
		return "", st
	}

	if user == "" {
		return "", newNoAuthStatus()
	}

	return user, nil
}

func (a AuthHandler) GetOboUserClusterAgent(
	ctx context.Context,
) (*gocbcorex.Agent, string, *status.Status) {
	oboUser, errSt := a.GetOboUserFromContext(ctx)
	if errSt != nil {
		return nil, "", errSt
	}

	clusterAgent := a.CbClient.GetClusterAgent()
	return clusterAgent, oboUser, nil
}

func (a AuthHandler) GetOboUserBucketAgent(
	ctx context.Context, bucketName string,
) (*gocbcorex.Agent, string, *status.Status) {
	oboUser, errSt := a.GetOboUserFromContext(ctx)
	if errSt != nil {
		return nil, "", errSt
	}

	bucketAgent, err := a.CbClient.GetBucketAgent(ctx, bucketName)
	if err != nil {
		return nil, "", cbGenericErrToPsStatus(err, a.Logger)
	}

	return bucketAgent, oboUser, nil
}

func (a AuthHandler) GetOboUserAgent(
	ctx context.Context, bucketName *string,
) (*gocbcorex.Agent, string, *status.Status) {
	if bucketName == nil {
		return a.GetOboUserClusterAgent(ctx)
	} else {
		return a.GetOboUserBucketAgent(ctx, *bucketName)
	}
}
