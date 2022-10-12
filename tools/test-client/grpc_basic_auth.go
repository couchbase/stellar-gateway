package gocbps

import (
	"context"
	"encoding/base64"

	"google.golang.org/grpc/credentials"
)

type grpcBasicAuth struct {
	EncodedData string
}

// NewJWTAccessFromKey creates PerRPCCredentials from the given jsonKey.
func newGrpcBasicAuth(username, password string) (credentials.PerRPCCredentials, error) {
	basicAuth := username + ":" + password
	authValue := base64.StdEncoding.EncodeToString([]byte(basicAuth))
	return grpcBasicAuth{authValue}, nil
}

func (j grpcBasicAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Basic " + j.EncodedData,
	}, nil
}

func (j grpcBasicAuth) RequireTransportSecurity() bool {
	return false
}
