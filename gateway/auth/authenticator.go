package auth

import (
	"context"
	"crypto/tls"
)

type Authenticator interface {
	ValidateUserForObo(ctx context.Context, user, pass string) (string, string, error)
	ValidateConnStateForObo(ctx context.Context, connState *tls.ConnectionState) (string, string, error)
}
