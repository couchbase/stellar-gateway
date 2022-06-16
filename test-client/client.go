package gocbps

import (
	"context"
	"crypto/x509"
	"log"
	"strings"

	"github.com/couchbase/stellar-nebula/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn            *grpc.ClientConn
	couchbaseClient protos.CouchbaseClient
	routingClient   protos.RoutingClient
}

type ConnectOptions struct {
	Username          string
	Password          string
	ClientCertificate *x509.CertPool
}

func Connect(connStr string, opts *ConnectOptions) (*Client, error) {
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

	// use port 18091 by default
	{
		connStrPieces := strings.Split(connStr, ":")
		if len(connStrPieces) == 1 {
			connStrPieces = append(connStrPieces, "18098")
		}
		connStr = strings.Join(connStrPieces, ":")
	}

	conn, err := grpc.Dial(connStr, transportDialOpt, perRpcDialOpt)
	if err != nil {
		return nil, err
	}

	couchbaseClient := protos.NewCouchbaseClient(conn)
	routingClient := protos.NewRoutingClient(conn)

	// this is our version of checking auth credentials
	_, err = couchbaseClient.Hello(context.Background(), &protos.HelloRequest{})
	if err != nil {
		closeErr := conn.Close()
		if closeErr != nil {
			log.Printf("failed to close connection after error: %s", closeErr)
		}

		return nil, err
	}

	return &Client{
		conn:            conn,
		couchbaseClient: couchbaseClient,
		routingClient:   routingClient,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Bucket(bucketName string) *Bucket {
	return &Bucket{
		client:     c,
		bucketName: bucketName,
	}
}

// INTERNAL: Used for testing
func (c *Client) GetConn() *grpc.ClientConn {
	return c.conn
}
