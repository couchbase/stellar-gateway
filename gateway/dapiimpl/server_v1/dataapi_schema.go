package server_v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"github.com/couchbase/stellar-gateway/dataapiv1"
)

func (s *DataApiServer) ListScopes(
	ctx context.Context, in dataapiv1.ListScopesRequestObject,
) (dataapiv1.ListScopesResponseObject, error) {
	clusterAgent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.Params.Authorization, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	result, err := clusterAgent.GetCollectionManifest(ctx, &cbmgmtx.GetCollectionManifestOptions{
		BucketName: in.BucketName,
		OnBehalfOf: oboInfo,
	})
	if err != nil {
		if errors.Is(err, cbmgmtx.ErrBucketNotFound) {
			return nil, s.errorHandler.NewBucketMissingStatus(err, in.BucketName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	var scopes []dataapiv1.ScopeInfo
	for _, scope := range result.Scopes {
		var collections []dataapiv1.CollectionInfo
		for _, collection := range scope.Collections {
			collections = append(collections, dataapiv1.CollectionInfo{
				Name: collection.Name,
			})
		}

		scopes = append(scopes, dataapiv1.ScopeInfo{
			Name:        scope.Name,
			Collections: collections,
		})
	}

	return dataapiv1.ListScopes200JSONResponse(scopes), nil
}

func (s *DataApiServer) ListCollections(
	ctx context.Context, in dataapiv1.ListCollectionsRequestObject,
) (dataapiv1.ListCollectionsResponseObject, error) {
	clusterAgent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.Params.Authorization, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	result, err := clusterAgent.GetCollectionManifest(ctx, &cbmgmtx.GetCollectionManifestOptions{
		BucketName: in.BucketName,
		OnBehalfOf: oboInfo,
	})
	if err != nil {
		if errors.Is(err, cbmgmtx.ErrBucketNotFound) {
			return nil, s.errorHandler.NewBucketMissingStatus(err, in.BucketName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	var foundScope *cbconfig.CollectionManifestScopeJson
	for _, scope := range result.Scopes {
		if scope.Name == in.ScopeName {
			foundScope = &scope
			break
		}
	}

	if foundScope == nil {
		return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
	}

	var collections []dataapiv1.CollectionInfo
	for _, collection := range foundScope.Collections {
		collections = append(collections, dataapiv1.CollectionInfo{
			Name: collection.Name,
		})
	}

	return dataapiv1.ListCollections200JSONResponse(collections), nil
}

func (s *DataApiServer) InferSchemas(
	ctx context.Context, in dataapiv1.InferSchemasRequestObject,
) (dataapiv1.InferSchemasResponseObject, error) {
	clusterAgent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, in.Params.Authorization, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	result, err := clusterAgent.Query(ctx, &gocbcorex.QueryOptions{
		Statement:  fmt.Sprintf("INFER `%s`.`%s`.`%s`", in.BucketName, in.ScopeName, in.CollectionName),
		OnBehalfOf: oboInfo,
	})
	if err != nil {
		if errors.Is(err, cbqueryx.ErrScopeNotFound) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		}
		if errors.Is(err, cbqueryx.ErrCollectionNotFound) {
			return nil, s.errorHandler.NewCollectionMissingStatus(
				err,
				in.BucketName,
				in.ScopeName,
				in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	var schemasJson json.RawMessage = nil
	for result.HasMoreRows() {
		row, err := result.ReadRow()
		if err != nil {
			return nil, s.errorHandler.NewGenericStatus(err).Err()
		}

		schemasJson = row
	}
	if schemasJson == nil {
		return nil, s.errorHandler.NewGenericStatus(errors.New("schema missing from infer results")).Err()
	}

	var schemas []interface{}
	err = json.Unmarshal(schemasJson, &schemas)
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return dataapiv1.InferSchemas200JSONResponse(dataapiv1.InferSchemas200JSONResponse(schemas)), nil
}
