package servers

import (
	"crypto/tls"

	query_v1 "github.com/couchbase/stellar-nebula/genproto/query/v1"
)

type QueryServerOptions struct {
	Port      int
	TlsConfig *tls.Config

	QueryServer query_v1.QueryServer
}

type QueryServer struct {
}
