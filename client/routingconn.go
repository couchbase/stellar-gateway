package client

import (
	"crypto/x509"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/couchbase/goprotostellar/genproto/routing_v1"
	"github.com/couchbase/stellar-gateway/contrib/grpcheaderauth"
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
	routingV1 routing_v1.RoutingServiceClient
	kvV1      kv_v1.KvServiceClient
	queryV1   query_v1.QueryServiceClient
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
		basicAuthCreds, err := grpcheaderauth.NewGrpcBasicAuth(opts.Username, opts.Password)
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

	conn, err := grpc.NewClient(address, dialOpts...)
	if err != nil {
		return nil, err
	}

	return &routingConn{
		conn:      conn,
		routingV1: routing_v1.NewRoutingServiceClient(conn),
		kvV1:      kv_v1.NewKvServiceClient(conn),
		queryV1:   query_v1.NewQueryServiceClient(conn),
	}, nil
}

func (c *routingConn) RoutingV1() routing_v1.RoutingServiceClient {
	return c.routingV1
}

func (c *routingConn) KvV1() kv_v1.KvServiceClient {
	return c.kvV1
}

func (c *routingConn) QueryV1() query_v1.QueryServiceClient {
	return c.queryV1
}
