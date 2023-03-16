package app_config

import "flag"

var (
	cbHost     = flag.String("cb-host", "127.0.0.1", "the couchbase server host")
	cbUser     = flag.String("cb-user", "Administrator", "the couchbase server username")
	cbPass     = flag.String("cb-pass", "password", "the couchbase server password")
	dataPort   = flag.Int("data-port", 18098, "the data port")
	sdPort     = flag.Int("sd-port", 18099, "the sd port")
	webPort    = flag.Int("web-port", 9091, "the web metrics/health port")
	daemon     = flag.Bool("daemon", false, "When in daemon mode, stellar-gateway will restart on failure")
	clientCert = flag.String("client-cert", "", "Couchbase Client certificate for mtls ")
	clientKey  = flag.String("client-key", "", "Couchbase client key for mtls")
	serverCert = flag.String("server-cert", "", "Certificate for TLS for external hosted elements")
	serverKey  = flag.String("server-key", "", "Key for TLS for external hosted elements")
)