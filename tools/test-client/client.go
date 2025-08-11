package gocbps

import (
	"crypto/tls"
	"crypto/x509"
	"strings"

	"github.com/couchbase/goprotostellar/genproto/analytics_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/couchbase/goprotostellar/genproto/search_v1"
	"github.com/couchbase/goprotostellar/genproto/view_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Client struct {
	conn            *grpc.ClientConn
	kvClient        kv_v1.KvServiceClient
	queryClient     query_v1.QueryServiceClient
	searchClient    search_v1.SearchServiceClient
	analyticsClient analytics_v1.AnalyticsServiceClient
	viewClient      view_v1.ViewServiceClient
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

		transportDialOpt = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		}))
		perRpcDialOpt = grpc.WithPerRPCCredentials(basicAuthCreds)
	} else {
		transportDialOpt = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		}))
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

	dialOpts = append(dialOpts, grpc.WithUserAgent("gocbps/v1.0.0 (grpc/vX.Y.Z)"))

	conn, err := grpc.NewClient(connStr, dialOpts...)
	if err != nil {
		return nil, err
	}

	kvClient := kv_v1.NewKvServiceClient(conn)
	queryClient := query_v1.NewQueryServiceClient(conn)
	searchClient := search_v1.NewSearchServiceClient(conn)
	analyticsClient := analytics_v1.NewAnalyticsServiceClient(conn)
	viewClient := view_v1.NewViewServiceClient(conn)

	return &Client{
		conn:            conn,
		kvClient:        kvClient,
		queryClient:     queryClient,
		searchClient:    searchClient,
		analyticsClient: analyticsClient,
		viewClient:      viewClient,
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
