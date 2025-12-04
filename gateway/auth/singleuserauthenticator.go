package auth

import (
	"context"
	"crypto/tls"
	"errors"
)

// We intentionally use an error for this case so that we only permit non-obo requests
// to be produced if the caller is explicitly checking for this condition.
var ErrSingleUserAuthValid = errors.New("single user authentication successful")

type SingleUserAuthenticator struct {
	Username string
	Password string
}

func (a *SingleUserAuthenticator) ValidateUserForObo(ctx context.Context, user, pass string) (string, string, error) {
	if user == a.Username && pass == a.Password {
		return "", "", ErrSingleUserAuthValid
	}

	return "", "", ErrInvalidCredentials
}

func (a *SingleUserAuthenticator) ValidateConnStateForObo(ctx context.Context, connState *tls.ConnectionState) (string, string, error) {
	return "", "", ErrInvalidCertificate
}
