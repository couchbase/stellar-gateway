package auth

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocbcorex/cbauthx"
	"go.uber.org/zap"
)

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrInvalidCertificate = errors.New("invalid certificate")
	ErrCertAuthDisabled   = errors.New("client cert auth disabled")
)

type CbAuthAuthenticator struct {
	Authenticator *cbauthx.CbAuth
}

var _ Authenticator = (*CbAuthAuthenticator)(nil)

type NewCbAuthAuthenticatorOptions struct {
	Logger      *zap.Logger
	NodeId      string
	ClusterUUID string
	Addresses   []string
	Username    string
	Password    string
}

func rewriteCbAuthAddresses(addresses []string) []string {
	out := make([]string, 0, len(addresses))
	for _, address := range addresses {
		out = append(out, "http://"+address)
	}
	return out
}

func NewCbAuthAuthenticator(ctx context.Context, opts NewCbAuthAuthenticatorOptions) (*CbAuthAuthenticator, error) {
	shortNodeId := opts.NodeId
	if len(shortNodeId) > 8 {
		shortNodeId = shortNodeId[:8]
	}

	auth, err := cbauthx.NewCbAuth(ctx, &cbauthx.CbAuthConfig{
		Endpoints:   rewriteCbAuthAddresses(opts.Addresses),
		Username:    opts.Username,
		Password:    opts.Password,
		ClusterUuid: opts.ClusterUUID,
	}, &cbauthx.CbAuthOptions{
		Logger:            opts.Logger,
		ServiceName:       "stg-" + shortNodeId,
		HeartbeatInterval: 5 * time.Second,
		HeartbeatTimeout:  15 * time.Second,
		LivenessTimeout:   20 * time.Second,
		ConnectTimeout:    5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &CbAuthAuthenticator{
		Authenticator: auth,
	}, nil
}

type CbAuthAuthenticatorReconfigureOptions struct {
	Addresses   []string
	Username    string
	Password    string
	ClusterUUID string
}

func (a *CbAuthAuthenticator) Reconfigure(opts CbAuthAuthenticatorReconfigureOptions) error {
	return a.Authenticator.Reconfigure(&cbauthx.CbAuthConfig{
		Endpoints:   rewriteCbAuthAddresses(opts.Addresses),
		Username:    opts.Username,
		Password:    opts.Password,
		ClusterUuid: opts.ClusterUUID,
	})
}

func (a *CbAuthAuthenticator) ValidateUserForObo(ctx context.Context, user, pass string) (string, string, error) {
	info, err := a.Authenticator.CheckUserPass(ctx, user, pass)
	if err != nil {
		if errors.Is(err, cbauthx.ErrInvalidAuth) {
			return "", "", ErrInvalidCredentials
		}

		return "", "", fmt.Errorf("failed to check credentials with cbauth: %s", err.Error())
	}

	return user, info.Domain, nil
}

func (a *CbAuthAuthenticator) ValidateConnStateForObo(ctx context.Context, connState *tls.ConnectionState) (string, string, error) {
	info, err := a.Authenticator.CheckCertificate(ctx, connState)
	if err != nil {
		if errors.Is(err, cbauthx.ErrInvalidAuth) {
			return "", "", ErrInvalidCertificate
		} else if errors.Is(err, cbauthx.ErrCertAuthDisabled) {
			return "", "", ErrCertAuthDisabled
		}

		return "", "", fmt.Errorf("failed to check certificate with cbauth: %w", err)
	}

	return info.User, info.Domain, nil
}

func (a *CbAuthAuthenticator) Close() error {
	return a.Authenticator.Close()
}
