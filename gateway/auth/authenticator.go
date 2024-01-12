package auth

import "context"

type Authenticator interface {
	ValidateUserForObo(ctx context.Context, user, pass string) (string, string, error)
}
