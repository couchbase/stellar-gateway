/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package apiversion

import (
	"context"
	"time"

	"google.golang.org/grpc/metadata"
)

func FromContext(ctx context.Context) (ApiVersion, error) {
	apiVersions := metadata.ValueFromIncomingContext(ctx, "X-API-Version")
	if len(apiVersions) == 0 {
		// if the user has not specified a version, use the latest
		return Latest, nil
	}

	apiVersionStr := apiVersions[len(apiVersions)-1]

	// Date Format from ISO 8601
	apiVersionTime, err := time.Parse("2006-01-02", apiVersionStr)
	if err != nil {
		return 0, err
	}

	apiVersion := ApiVersion(apiVersionTime.Year()*10000 +
		int(apiVersionTime.Month())*100 +
		apiVersionTime.Day()*1)

	return apiVersion, nil
}
