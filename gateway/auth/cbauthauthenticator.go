/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocbcorex/cbauthx"
	"go.uber.org/zap"
)

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
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

func (a *CbAuthAuthenticator) Close() error {
	return a.Authenticator.Close()
}
