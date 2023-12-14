package auth

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/clog"
	"github.com/couchbase/gocbcorex"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/slices"
)

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
)

type authenticatorState struct {
	Authenticator cbauth.ExternalAuthenticator
}

type CbAuthAuthenticator struct {
	authenticatorState gocbcorex.AtomicPointer[authenticatorState]

	lock           sync.Mutex
	waitSig        *sync.Cond
	closed         bool
	addresses      []string
	currentAddress string

	logger      *zap.Logger
	service     string
	clusterUUID string
	username    string
	password    string
}

var _ Authenticator = (*CbAuthAuthenticator)(nil)

type NewCbAuthAuthenticatorOptions struct {
	Service     string
	ClusterUUID string
	Addresses   []string
	Username    string
	Password    string
	Logger      *zap.Logger
}

func NewCbAuthAuthenticator(opts NewCbAuthAuthenticatorOptions) (*CbAuthAuthenticator, error) {
	a := &CbAuthAuthenticator{
		service:     opts.Service,
		clusterUUID: opts.ClusterUUID,
		addresses:   opts.Addresses,
		username:    opts.Username,
		password:    opts.Password,
		logger:      opts.Logger,
	}

	a.waitSig = sync.NewCond(&a.lock)

	clog.SetLoggerCallback(func(level, format string, args ...interface{}) string {
		var zapLevel zapcore.Level
		switch level {
		case "FATA":
			zapLevel = zap.ErrorLevel
		case "CRIT":
			zapLevel = zap.ErrorLevel
		case "ERRO":
			zapLevel = zap.ErrorLevel
		case "WARN":
			zapLevel = zap.WarnLevel
		case "INFO":
			zapLevel = zap.InfoLevel
		default:
			zapLevel = zap.DebugLevel
		}

		a.logger.Log(zapLevel,
			"cbauth message",
			zap.String("level", level),
			zap.String("message", fmt.Sprintf(format, args...)))

		// return blank so its not logged to the default logger
		return ""
	})

	a.initExternalAuthenticator()

	go a.runThread()

	return a, nil
}

func (a *CbAuthAuthenticator) initExternalAuthenticator() {
	var availableHosts []string
	var failedHosts []string

	for {
		a.lock.Lock()
		if a.closed {
			a.lock.Unlock()
			break
		}

		availableHosts = availableHosts[:0]
		for _, address := range a.addresses {
			if !slices.Contains(failedHosts, address) {
				availableHosts = append(availableHosts, address)
			}
		}

		if len(availableHosts) == 0 {
			a.lock.Unlock()

			a.logger.Debug("no more available hosts for cbauth to try, waiting...")
			failedHosts = failedHosts[:0]
			time.Sleep(1 * time.Second)
			continue
		}

		currentAddress := availableHosts[rand.Intn(len(availableHosts))]
		a.currentAddress = currentAddress
		a.lock.Unlock()

		a.logger.Debug("attempting to establish new cbauth connection",
			zap.String("address", currentAddress))

		err := cbauth.InitExternalWithHeartbeat(a.service, currentAddress, a.username, a.password, 5, 10)
		if err != nil {
			failedHosts = append(failedHosts, currentAddress)

			a.logger.Warn("failed to initialize cbauth",
				zap.Error(err))
			continue
		}

		auth := cbauth.GetExternalAuthenticator()

		err = auth.SetExpectedClusterUuid(a.clusterUUID)
		if err != nil {
			a.logger.Warn("failed to set expected cluster uuid, running without uuid validation",
				zap.Error(err))
		}

		a.logger.Debug("successfully connected cbauth to new host",
			zap.String("address", currentAddress))

		a.authenticatorState.Store(&authenticatorState{
			Authenticator: auth,
		})
		break
	}
}

func (a *CbAuthAuthenticator) runThread() {
	a.lock.Lock()

	for {
		if a.closed {
			break
		}

		a.waitSig.Wait()

		// if the current address is already in our config, we don't have
		// anything we actually need to do.
		if slices.Contains(a.addresses, a.currentAddress) {
			continue
		}

		a.lock.Unlock()

		a.initExternalAuthenticator()

		a.lock.Lock()
	}

	a.lock.Unlock()
}

type CbAuthAuthenticatorReconfigureOptions struct {
	Addresses []string
}

func (a *CbAuthAuthenticator) Reconfigure(opts CbAuthAuthenticatorReconfigureOptions) error {
	a.lock.Lock()
	a.addresses = opts.Addresses
	a.waitSig.Broadcast()
	a.lock.Unlock()
	return nil
}

func (a *CbAuthAuthenticator) ValidateUserForObo(user, pass string) (string, string, error) {
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

func (a *CbAuthAuthenticator) Close() error {
	a.lock.Lock()
	a.closed = true
	a.waitSig.Signal()
	a.lock.Unlock()

	return nil
}
