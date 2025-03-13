/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package grpcheaderauth

import (
	"context"
	"encoding/base64"

	"google.golang.org/grpc/credentials"
)

type GrpcBasicAuth struct {
	EncodedData string
}

// NewJWTAccessFromKey creates PerRPCCredentials from the given jsonKey.
func NewGrpcBasicAuth(username, password string) (credentials.PerRPCCredentials, error) {
	basicAuth := username + ":" + password
	authValue := base64.StdEncoding.EncodeToString([]byte(basicAuth))
	return GrpcBasicAuth{authValue}, nil
}

func (j GrpcBasicAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Basic " + j.EncodedData,
	}, nil
}

func (j GrpcBasicAuth) RequireTransportSecurity() bool {
	return false
}
