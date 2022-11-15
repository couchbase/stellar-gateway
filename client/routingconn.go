package client

import (
	"crypto/x509"

	"github.com/couchbase/stellar-nebula/genproto/kv_v1"
	"github.com/couchbase/stellar-nebula/genproto/query_v1"
	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type routingConnOptions struct {
	ClientCertificate *x509.CertPool
	Username          string
	Password          string
}

type routingConn struct {
	conn      *grpc.ClientConn
	routingV1 routing_v1.RoutingClient
	kvV1      kv_v1.KvClient
	queryV1   query_v1.QueryClient
}

// Verify that routingConn implements Conn
var _ Conn = (*routingConn)(nil)

func dialRoutingConn(address string, opts *routingConnOptions) (*routingConn, error) {
	var transportDialOpt grpc.DialOption
	var perRpcDialOpt grpc.DialOption

	if opts.ClientCertificate != nil {
		transportDialOpt = grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(opts.ClientCertificate, ""))
		perRpcDialOpt = nil
	} else if opts.Username != "" && opts.Password != "" {
		basicAuthCreds, err := newGrpcBasicAuth(opts.Username, opts.Password)
		if err != nil {
			return nil, err
		}

		transportDialOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
		perRpcDialOpt = grpc.WithPerRPCCredentials(basicAuthCreds)
	} else {
		transportDialOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
		perRpcDialOpt = nil
	}

	dialOpts := []grpc.DialOption{transportDialOpt}
	if perRpcDialOpt != nil {
		dialOpts = append(dialOpts, perRpcDialOpt)
	}

	conn, err := grpc.Dial(address, dialOpts...)
	if err != nil {
		return nil, err
	}

	return &routingConn{
		conn:      conn,
		routingV1: routing_v1.NewRoutingClient(conn),
		kvV1:      kv_v1.NewKvClient(conn),
		queryV1:   query_v1.NewQueryClient(conn),
	}, nil
}

func (c *routingConn) RoutingV1() routing_v1.RoutingClient {
	return c.routingV1
}

func (c *routingConn) KvV1() kv_v1.KvClient {
	return c.kvV1
}

func (c *routingConn) QueryV1() query_v1.QueryClient {
	return c.queryV1
}
