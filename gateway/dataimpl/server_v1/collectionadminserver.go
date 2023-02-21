package server_v1

import (
	"context"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"google.golang.org/grpc/status"
)

type CollectionAdminServer struct {
	admin_collection_v1.UnimplementedCollectionAdminServer

	cbClient *gocbcorex.AgentManager
}

func (s *CollectionAdminServer) getBucketAgent(
	ctx context.Context, bucketName string,
) (*gocbcorex.Agent, *status.Status) {
	bucketAgent, err := s.cbClient.GetBucketAgent(ctx, bucketName)
	if err != nil {
		return nil, cbGenericErrToPsStatus(err)
	}
	return bucketAgent, nil
}

func (s *CollectionAdminServer) ListCollections(
	ctx context.Context,
	in *admin_collection_v1.ListCollectionsRequest,
) (*admin_collection_v1.ListCollectionsResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	result, err := bucketAgent.GetCollectionManifest(ctx, &cbmgmtx.GetCollectionManifestOptions{
		BucketName: in.BucketName,
	})
	if err != nil {
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	var scopes []*admin_collection_v1.ListCollectionsResponse_Scope
	for _, scope := range result.Scopes {
		var collections []*admin_collection_v1.ListCollectionsResponse_Collection

		for _, collection := range scope.Collections {
			collections = append(collections, &admin_collection_v1.ListCollectionsResponse_Collection{
				Name: collection.Name,
			})
		}

		scopes = append(scopes, &admin_collection_v1.ListCollectionsResponse_Scope{
			Name:        scope.Name,
			Collections: collections,
		})
	}

	return &admin_collection_v1.ListCollectionsResponse{
		Scopes: scopes,
	}, nil
}

func (s *CollectionAdminServer) CreateScope(
	ctx context.Context,
	in *admin_collection_v1.CreateScopeRequest,
) (*admin_collection_v1.CreateScopeResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	err := bucketAgent.CreateScope(ctx, &cbmgmtx.CreateScopeOptions{
		BucketName: in.BucketName,
		ScopeName:  in.ScopeName,
	})
	if err != nil {
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &admin_collection_v1.CreateScopeResponse{}, nil
}

func (s *CollectionAdminServer) DeleteScope(
	ctx context.Context,
	in *admin_collection_v1.DeleteScopeRequest,
) (*admin_collection_v1.DeleteScopeResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	err := bucketAgent.DeleteScope(ctx, &cbmgmtx.DeleteScopeOptions{
		BucketName: in.BucketName,
		ScopeName:  in.ScopeName,
	})
	if err != nil {
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &admin_collection_v1.DeleteScopeResponse{}, nil
}

func (s *CollectionAdminServer) CreateCollection(
	ctx context.Context,
	in *admin_collection_v1.CreateCollectionRequest,
) (*admin_collection_v1.CreateCollectionResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	err := bucketAgent.CreateCollection(ctx, &cbmgmtx.CreateCollectionOptions{
		BucketName:     in.BucketName,
		CollectionName: in.CollectionName,
		ScopeName:      in.ScopeName,
		// TODO(brett19): Implement collection max expiry
		MaxTTL: 0,
	})
	if err != nil {
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &admin_collection_v1.CreateCollectionResponse{}, nil
}

func (s *CollectionAdminServer) DeleteCollection(
	ctx context.Context,
	in *admin_collection_v1.DeleteCollectionRequest,
) (*admin_collection_v1.DeleteCollectionResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	err := bucketAgent.DeleteCollection(ctx, &cbmgmtx.DeleteCollectionOptions{
		BucketName:     in.BucketName,
		ScopeName:      in.ScopeName,
		CollectionName: in.CollectionName,
	})
	if err != nil {
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &admin_collection_v1.DeleteCollectionResponse{}, nil
}

func NewCollectionAdminServer(cbClient *gocbcorex.AgentManager) *CollectionAdminServer {
	return &CollectionAdminServer{
		cbClient: cbClient,
	}
}
