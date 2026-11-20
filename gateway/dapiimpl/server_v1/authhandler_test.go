package server_v1

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"testing"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/stellar-gateway/gateway/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type fakeAuthenticator struct {
	user, domain string
	err          error
}

func (f fakeAuthenticator) ValidateUserForObo(_ context.Context, _, _ string) (string, string, error) {
	return f.user, f.domain, f.err
}

func (f fakeAuthenticator) ValidateConnStateForObo(_ context.Context, _ *tls.ConnectionState) (string, string, error) {
	return f.user, f.domain, f.err
}

func newTestAuthHandler(authenticator auth.Authenticator) AuthHandler {
	logger := zap.NewNop()
	return AuthHandler{
		Logger:        logger,
		ErrorHandler:  &ErrorHandler{Logger: logger},
		Authenticator: authenticator,
	}
}

func basicAuthHdr(user, pass string) *string {
	hdr := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
	return &hdr
}

func certCtx() context.Context {
	return context.WithValue(context.Background(), CtxKeyTlsConnState{}, &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{{}},
	})
}

func TestGetMemdOboUserFromRequest(t *testing.T) {
	testCases := []struct {
		name     string
		ctx      context.Context
		authHdr  *string
		authUser string
		domain   string
		authErr  error
		expected *gocbcorex.OnBehalfOfInfo
	}{
		{name: "password local", ctx: context.Background(), authHdr: basicAuthHdr("alice", "pass"), authUser: "alice", domain: "local", expected: &gocbcorex.OnBehalfOfInfo{Username: "alice", Domain: "local"}},
		{name: "password admin", ctx: context.Background(), authHdr: basicAuthHdr("Administrator", "pass"), authUser: "Administrator", domain: "admin", expected: &gocbcorex.OnBehalfOfInfo{Username: "Administrator", Domain: "admin"}},
		// The domain must be carried through to gocbcorex, otherwise memcached
		// would resolve an external user as a same-named local user.
		{name: "password external", ctx: context.Background(), authHdr: basicAuthHdr("alice", "pass"), authUser: "alice", domain: "external", expected: &gocbcorex.OnBehalfOfInfo{Username: "alice", Domain: "external"}},
		{name: "cert local", ctx: certCtx(), authUser: "alice", domain: "local", expected: &gocbcorex.OnBehalfOfInfo{Username: "alice", Domain: "local"}},
		{name: "cert external", ctx: certCtx(), authUser: "alice", domain: "external", expected: &gocbcorex.OnBehalfOfInfo{Username: "alice", Domain: "external"}},
		{name: "single user auth", ctx: context.Background(), authHdr: basicAuthHdr("Administrator", "pass"), authErr: auth.ErrSingleUserAuthValid, expected: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestAuthHandler(fakeAuthenticator{user: tc.authUser, domain: tc.domain, err: tc.authErr})

			oboUser, st := h.GetMemdOboUserFromRequest(tc.ctx, tc.authHdr)
			require.Nil(t, st)
			assert.Equal(t, tc.expected, oboUser)
		})
	}
}

func TestGetMemdOboUserFromRequestInvalidCredentials(t *testing.T) {
	h := newTestAuthHandler(fakeAuthenticator{err: auth.ErrInvalidCredentials})

	oboUser, st := h.GetMemdOboUserFromRequest(context.Background(), basicAuthHdr("alice", "wrong"))
	require.NotNil(t, st)
	assert.Nil(t, oboUser)
}

// A local user whose name begins with '^' would be resolved by memcached as an
// external user, so it must be rejected rather than forwarded.
func TestGetMemdOboUserFromRequestUnrepresentableUser(t *testing.T) {
	h := newTestAuthHandler(fakeAuthenticator{user: "^bob", domain: "local"})

	oboUser, st := h.GetMemdOboUserFromRequest(context.Background(), basicAuthHdr("^bob", "pass"))
	require.NotNil(t, st)
	assert.Nil(t, oboUser)
}
