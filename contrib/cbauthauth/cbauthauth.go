package cbauthauth

import (
	"crypto/tls"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/gocbcorex"
	"go.uber.org/zap"
)

// CbAuthAuthenticator implements an authenticator which uses cbauth to fetch
// internal Couchbase service credentials to execute with.
type CbAuthAuthenticator struct {
	logger *zap.Logger
}

var _ gocbcorex.Authenticator = (*CbAuthAuthenticator)(nil)

func (a CbAuthAuthenticator) GetClientCertificate(service gocbcorex.ServiceType, hostPort string) (*tls.Certificate, error) {
	return nil, nil
}

func (a CbAuthAuthenticator) GetCredentials(service gocbcorex.ServiceType, hostPort string) (string, string, error) {
	if service == gocbcorex.ServiceTypeMemd {
		user, pass, err := cbauth.GetMemcachedServiceAuth(hostPort)

		if a.logger != nil {
			a.logger.Debug("requested memd credentials from cbauth",
				zap.Error(err),
				zap.String("service", service.String()),
				zap.String("hostPort", hostPort),
				zap.String("user", user))
		}

		return user, pass, err
	} else {
		user, pass, err := cbauth.GetHTTPServiceAuth(hostPort)

		if a.logger != nil {
			a.logger.Debug("requested http credentials from cbauth",
				zap.Error(err),
				zap.String("service", service.String()),
				zap.String("hostPort", hostPort),
				zap.String("user", user))
		}

		return user, pass, err
	}
}
