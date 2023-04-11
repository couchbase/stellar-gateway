package auth

import (
	"errors"
	"fmt"

	"github.com/couchbase/cbauth"
)

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
)

type CbAuthAuthenticator struct {
}

var _ Authenticator = (*CbAuthAuthenticator)(nil)

func (a CbAuthAuthenticator) ValidateUserForObo(user, pass string) (string, string, error) {
	creds, err := cbauth.Auth(user, pass)
	if err != nil {
		if errors.Is(err, cbauth.ErrNoAuth) {
			return "", "", ErrInvalidCredentials
		}

		return "", "", fmt.Errorf("failed to check credentials with cbauth: %s", err.Error())
	}

	username, domain := creds.User()
	return username, domain, nil
}
