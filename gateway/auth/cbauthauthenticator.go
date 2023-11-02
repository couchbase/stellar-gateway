package auth

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/revrpc"
	"github.com/couchbase/gocbcorex"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
)

type authenticatorState struct {
	Authenticator cbauth.ExternalAuthenticator
	HostPort      string
}

type CbAuthAuthenticator struct {
	authenticatorState gocbcorex.AtomicPointer[authenticatorState]

	logger   *zap.Logger
	username string
	password string
	service  string
}

var _ Authenticator = (*CbAuthAuthenticator)(nil)

type NewCbAuthAuthenticatorOptions struct {
	Username      string
	Password      string
	BootstrapHost string
	Service       string
	Logger        *zap.Logger
}

func NewCbAuthAuthenticator(opts NewCbAuthAuthenticatorOptions) (CbAuthAuthenticator, error) {
	a := CbAuthAuthenticator{
		username: opts.Username,
		password: opts.Password,
		service:  opts.Service,
		logger:   opts.Logger,
	}

	revrpc.DefaultBabysitErrorPolicy = revrpc.DefaultErrorPolicy{
		RestartsToExit:       -1,
		SleepBetweenRestarts: time.Second,
		LogPrint: func(v ...any) {
			a.logger.Info("cbauth message",
				zap.String("message", fmt.Sprint(v...)))
		},
	}

	err := cbauth.InitExternalWithHeartbeat(a.service, opts.BootstrapHost, a.username, a.password, 5, 10)
	if err != nil {
		return CbAuthAuthenticator{}, err
	}

	auth := cbauth.GetExternalAuthenticator()
	a.authenticatorState.Store(&authenticatorState{
		Authenticator: auth,
		HostPort:      opts.BootstrapHost,
	})

	return a, nil
}

type CbAuthAuthenticatorReconfigureOptions struct {
	Addresses *gocbcorex.ParsedConfigAddresses
}

func (a CbAuthAuthenticator) Reconfigure(opts CbAuthAuthenticatorReconfigureOptions) error {
	if len(opts.Addresses.NonSSL.Mgmt) == 0 {
		return errors.New("reconfigure called with no mgmt addresses")
	}
	currentAuth := a.authenticatorState.Load()
	if currentAuth != nil {
		if slices.Contains(opts.Addresses.NonSSL.Mgmt, currentAuth.HostPort) {
			// Nothing to do here.
			return nil
		}
	}

	// We need to pick a new node, as ours went away.
	address := opts.Addresses.NonSSL.Mgmt[0]

	a.logger.Debug("Switching cbauth host", zap.String("new-host", address))

	err := cbauth.InitExternalWithHeartbeat(a.service, address, a.username, a.password, 5, 10)
	if err != nil {
		if strings.Contains(err.Error(), "already initialized") {
			// we ignore this error
			return nil
		} else {
			return err
		}
	}

	auth := cbauth.GetExternalAuthenticator()
	if !a.authenticatorState.CompareAndSwap(currentAuth, &authenticatorState{
		Authenticator: auth,
		HostPort:      address,
	}) {
		// This is unexpected and we could be in a weird position here.
		return errors.New("reconfigure was called concurrently")
	}

	return nil
}

func (a CbAuthAuthenticator) ValidateUserForObo(user, pass string) (string, string, error) {
	auth := a.authenticatorState.Load()
	if auth == nil {
		return "", "", errors.New("authenticator is not initialized")
	}
	creds, err := auth.Authenticator.Auth(user, pass)
	if err != nil {
		if errors.Is(err, cbauth.ErrNoAuth) {
			return "", "", ErrInvalidCredentials
		}

		return "", "", fmt.Errorf("failed to check credentials with cbauth: %s", err.Error())
	}

	username, domain := creds.User()
	return username, domain, nil
}
