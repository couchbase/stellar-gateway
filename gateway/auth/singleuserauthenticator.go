package auth

import (
	"context"
	"crypto/tls"
)

type SingleUserAuthenticator struct {
	Username string
	Password string
}

func (a *SingleUserAuthenticator) ValidateUserForObo(ctx context.Context, user, pass string) (string, string, error) {
	if user == a.Username && pass == a.Password {
		return user, "local", nil
	}

	return "", "", ErrInvalidCredentials
}

func (a *SingleUserAuthenticator) ValidateConnStateForObo(ctx context.Context, connState *tls.ConnectionState) (string, string, error) {
	return "", "", ErrInvalidCertificate
}
