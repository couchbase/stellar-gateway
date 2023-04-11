package auth

type Authenticator interface {
	ValidateUserForObo(user, pass string) (string, string, error)
}
