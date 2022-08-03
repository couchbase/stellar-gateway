package gocbps

import (
	"context"
	"crypto/x509"
	"log"
	"strings"

	analytics_v1 "github.com/couchbase/stellar-nebula/genproto/analytics/v1"
	data_v1 "github.com/couchbase/stellar-nebula/genproto/data/v1"
	query_v1 "github.com/couchbase/stellar-nebula/genproto/query/v1"
	routing_v1 "github.com/couchbase/stellar-nebula/genproto/routing/v1"
	search_v1 "github.com/couchbase/stellar-nebula/genproto/search/v1"
	couchbase_v1 "github.com/couchbase/stellar-nebula/genproto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn            *grpc.ClientConn
	couchbaseClient couchbase_v1.CouchbaseClient
	routingClient   routing_v1.RoutingClient
	dataClient      data_v1.DataClient
	queryClient     query_v1.QueryClient
	searchClient    search_v1.SearchClient
	analyticsClient analytics_v1.AnalyticsClient
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

	dialOpts := []grpc.DialOption{transportDialOpt}
	if perRpcDialOpt != nil {
		dialOpts = append(dialOpts, perRpcDialOpt)
	}

	conn, err := grpc.Dial(connStr, dialOpts...)
	if err != nil {
		return nil, err
	}

	couchbaseClient := couchbase_v1.NewCouchbaseClient(conn)
	routingClient := routing_v1.NewRoutingClient(conn)
	dataClient := data_v1.NewDataClient(conn)
	queryClient := query_v1.NewQueryClient(conn)
	searchClient := search_v1.NewSearchClient(conn)
	analyticsClient := analytics_v1.NewAnalyticsClient(conn)

	// this is our version of checking auth credentials
	_, err = couchbaseClient.Hello(context.Background(), &couchbase_v1.HelloRequest{})
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
		dataClient:      dataClient,
		queryClient:     queryClient,
		searchClient:    searchClient,
		analyticsClient: analyticsClient,
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
