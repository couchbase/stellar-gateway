package servers

import (
	"crypto/tls"

	"github.com/couchbase/stellar-nebula/genproto/query_v1"
)

type QueryServerOptions struct {
	Port      int
	TlsConfig *tls.Config

	QueryServer query_v1.QueryServer
}

type QueryServer struct {
}
