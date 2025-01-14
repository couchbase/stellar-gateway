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
