package server_v1

import (
	"context"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
)

type CollectionAdminServer struct {
	admin_collection_v1.UnimplementedCollectionAdminServer

	cbClient *gocb.Cluster
}

func (s *CollectionAdminServer) ListCollections(context context.Context, in *admin_collection_v1.ListCollectionsRequest) (*admin_collection_v1.ListCollectionsResponse, error) {
	collMgr := s.cbClient.Bucket(in.BucketName).Collections()

	result, err := collMgr.GetAllScopes(&gocb.GetAllScopesOptions{
		Context: context,
	})
	if err != nil {
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	var scopes []*admin_collection_v1.ListCollectionsResponse_Scope
	for _, scope := range result {
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

func (s *CollectionAdminServer) CreateScope(context context.Context, in *admin_collection_v1.CreateScopeRequest) (*admin_collection_v1.CreateScopeResponse, error) {
	collMgr := s.cbClient.Bucket(in.BucketName).Collections()

	err := collMgr.CreateScope(in.ScopeName, &gocb.CreateScopeOptions{
		Context: context,
	})
	if err != nil {
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &admin_collection_v1.CreateScopeResponse{}, nil
}

func (s *CollectionAdminServer) DeleteScope(context context.Context, in *admin_collection_v1.DeleteScopeRequest) (*admin_collection_v1.DeleteScopeResponse, error) {
	collMgr := s.cbClient.Bucket(in.BucketName).Collections()

	err := collMgr.DropScope(in.ScopeName, &gocb.DropScopeOptions{
		Context: context,
	})
	if err != nil {
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &admin_collection_v1.DeleteScopeResponse{}, nil
}

func (s *CollectionAdminServer) CreateCollection(context context.Context, in *admin_collection_v1.CreateCollectionRequest) (*admin_collection_v1.CreateCollectionResponse, error) {
	collMgr := s.cbClient.Bucket(in.BucketName).Collections()

	err := collMgr.CreateCollection(gocb.CollectionSpec{
		Name:      in.CollectionName,
		ScopeName: in.ScopeName,
		MaxExpiry: 0,
	}, &gocb.CreateCollectionOptions{
		Context: context,
	})
	if err != nil {
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &admin_collection_v1.CreateCollectionResponse{}, nil
}

func (s *CollectionAdminServer) DeleteCollection(context context.Context, in *admin_collection_v1.DeleteCollectionRequest) (*admin_collection_v1.DeleteCollectionResponse, error) {
	collMgr := s.cbClient.Bucket(in.BucketName).Collections()

	err := collMgr.DropCollection(gocb.CollectionSpec{
		Name:      in.CollectionName,
		ScopeName: in.ScopeName,
	}, &gocb.DropCollectionOptions{
		Context: context,
	})
	if err != nil {
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &admin_collection_v1.DeleteCollectionResponse{}, nil
}

func NewCollectionAdminServer(cbClient *gocb.Cluster) *CollectionAdminServer {
	return &CollectionAdminServer{
		cbClient: cbClient,
	}
}
