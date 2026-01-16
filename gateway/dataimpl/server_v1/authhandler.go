package server_v1

import (
	"context"
	"crypto/tls"
	"errors"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/stellar-gateway/gateway/auth"
	"github.com/couchbase/stellar-gateway/utils/authhdr"
	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type AuthHandler struct {
	Logger        *zap.Logger
	ErrorHandler  *ErrorHandler
	Authenticator auth.Authenticator
	CbClient      *gocbcorex.BucketsTrackingAgentManager
}

func (a AuthHandler) MaybeGetUserPassFromContext(ctx context.Context) (string, string, *status.Status) {
	authValues := metadata.ValueFromIncomingContext(ctx, "Authorization")
	if len(authValues) > 1 {
		a.Logger.Debug("more than a single authorization header was found")
		return "", "", a.ErrorHandler.NewInvalidAuthHeaderStatus(
			errors.New("more than a single authorization header was found"))
	}

	if len(authValues) == 0 {
		return "", "", nil
	}

	authValue := authValues[len(authValues)-1]

	username, password, ok := authhdr.DecodeBasicAuth(authValue)
	if !ok {
		a.Logger.Debug("failed to parse authorization header")
		return "", "", a.ErrorHandler.NewInvalidAuthHeaderStatus(
			errors.New("failed to parse authorization header"))
	}

	return username, password, nil
}

func (a AuthHandler) MaybeGetConnStateFromContext(ctx context.Context) (*tls.ConnectionState, *status.Status) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, nil
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		a.Logger.Debug("unexpected auth type", zap.String("authType", p.AuthInfo.AuthType()))
		return nil, a.ErrorHandler.NewUnexpectedAuthTypeStatus()
	}

	return &tlsInfo.State, nil
}

func (a AuthHandler) MaybeGetOboUserFromContext(ctx context.Context) (string, string, *status.Status) {
	username, password, errSt := a.MaybeGetUserPassFromContext(ctx)
	if errSt != nil {
		return "", "", errSt
	}

	connState, errSt := a.MaybeGetConnStateFromContext(ctx)
	if errSt != nil {
		return "", "", errSt
	}

	credsFound := username != "" && password != ""
	certFound := connState != nil && len(connState.PeerCertificates) != 0

	switch {
	case !credsFound && !certFound:
		return "", "", a.ErrorHandler.NewNoAuthStatus()
	case credsFound && certFound:
		a.Logger.Debug("username/password taking priority over client cert auth as both were given.")
	case credsFound:
	case certFound:
		oboUser, oboDomain, err := a.Authenticator.ValidateConnStateForObo(ctx, connState)
		if err != nil {
			if errors.Is(err, auth.ErrInvalidCertificate) {
				return "", "", a.ErrorHandler.NewInvalidCertificateStatus()
			} else if errors.Is(err, auth.ErrCertAuthDisabled) {
				return "", "", a.ErrorHandler.NewCertAuthDisabledStatus()
			}

			a.Logger.Error("received an unexpected cert authentication error", zap.Error(err))
			return "", "", a.ErrorHandler.NewInternalStatus()
		}

		return oboUser, oboDomain, nil
	}

	oboUser, oboDomain, err := a.Authenticator.ValidateUserForObo(ctx, username, password)
	if err != nil {
		if errors.Is(err, auth.ErrSingleUserAuthValid) {
			return "", "", nil
		}

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

	return user, domain, nil
}

func (a AuthHandler) GetHttpOboInfoFromContext(ctx context.Context) (*cbhttpx.OnBehalfOfInfo, *status.Status) {
	username, password, errSt := a.MaybeGetUserPassFromContext(ctx)
	if errSt != nil {
		return nil, errSt
	}

	connState, errHe := a.MaybeGetConnStateFromContext(ctx)
	if errHe != nil {
		return nil, errHe
	}

	credsFound := username != "" && password != ""
	certFound := connState != nil && len(connState.PeerCertificates) != 0

	switch {
	case !credsFound && !certFound:
		return nil, a.ErrorHandler.NewNoAuthStatus()
	case credsFound && certFound:
		a.Logger.Debug("username/password taking priority over client cert auth as both were given.")
	case credsFound:
	case certFound:
		oboUser, oboDomain, err := a.Authenticator.ValidateConnStateForObo(ctx, connState)
		if err != nil {
			if errors.Is(err, auth.ErrInvalidCertificate) {
				return nil, a.ErrorHandler.NewInvalidCertificateStatus()
			}

			a.Logger.Error("received an unexpected cert authentication error", zap.Error(err))
			return nil, a.ErrorHandler.NewInternalStatus()
		}

		return &cbhttpx.OnBehalfOfInfo{
			Username: oboUser,
			Domain:   oboDomain,
		}, nil
	}

	return &cbhttpx.OnBehalfOfInfo{
		Username: username,
		Password: password,
	}, nil
}

func (a AuthHandler) getClusterAgent(ctx context.Context) (*gocbcorex.Agent, *status.Status) {
	agent, err := a.CbClient.GetClusterAgent(ctx)
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
	/*
		oboUser, _, errSt := a.GetOboUserFromContext(ctx)
		if errSt != nil {
			return nil, "", errSt
		}
	*/
	oboUser := "Administrator"

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
