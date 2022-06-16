package server

import (
	"context"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/stellar-nebula/protos"
)

type bucketAdminServer struct {
	protos.UnimplementedBucketAdminServer

	cbClient *gocb.Cluster
}

func (s *bucketAdminServer) ListCollections(context context.Context, in *protos.ListCollectionsRequest) (*protos.ListCollectionsResponse, error) {
	collMgr := s.cbClient.Bucket(in.BucketName).Collections()

	result, err := collMgr.GetAllScopes(&gocb.GetAllScopesOptions{
		Context: context,
	})
	if err != nil {
		return nil, cbErrToPs(err)
	}

	var scopes []*protos.ListCollectionsResponse_Scope
	for _, scope := range result {
		var collections []*protos.ListCollectionsResponse_Collection

		for _, collection := range scope.Collections {
			collections = append(collections, &protos.ListCollectionsResponse_Collection{
				Name: collection.Name,
			})
		}

		scopes = append(scopes, &protos.ListCollectionsResponse_Scope{
			Name:        scope.Name,
			Collections: collections,
		})
	}

	return &protos.ListCollectionsResponse{
		Scopes: scopes,
	}, nil
}

func (s *bucketAdminServer) CreateScope(context context.Context, in *protos.CreateScopeRequest) (*protos.CreateScopeResponse, error) {
	collMgr := s.cbClient.Bucket(in.BucketName).Collections()

	err := collMgr.CreateScope(in.ScopeName, &gocb.CreateScopeOptions{
		Context: context,
	})
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &protos.CreateScopeResponse{}, nil
}

func (s *bucketAdminServer) DeleteScope(context context.Context, in *protos.DeleteScopeRequest) (*protos.DeleteScopeResponse, error) {
	collMgr := s.cbClient.Bucket(in.BucketName).Collections()

	err := collMgr.DropScope(in.ScopeName, &gocb.DropScopeOptions{
		Context: context,
	})
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &protos.DeleteScopeResponse{}, nil
}

func (s *bucketAdminServer) CreateCollection(context context.Context, in *protos.CreateCollectionRequest) (*protos.CreateCollectionResponse, error) {
	collMgr := s.cbClient.Bucket(in.BucketName).Collections()

	err := collMgr.CreateCollection(gocb.CollectionSpec{
		Name:      in.CollectionName,
		ScopeName: in.ScopeName,
		MaxExpiry: 0,
	}, &gocb.CreateCollectionOptions{
		Context: context,
	})
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &protos.CreateCollectionResponse{}, nil
}

func (s *bucketAdminServer) DeleteCollection(context context.Context, in *protos.DeleteCollectionRequest) (*protos.DeleteCollectionResponse, error) {
	collMgr := s.cbClient.Bucket(in.BucketName).Collections()

	err := collMgr.DropCollection(gocb.CollectionSpec{
		Name:      in.CollectionName,
		ScopeName: in.ScopeName,
	}, &gocb.DropCollectionOptions{
		Context: context,
	})
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &protos.DeleteCollectionResponse{}, nil
}

func NewBucketAdminServer(cbClient *gocb.Cluster) *bucketAdminServer {
	return &bucketAdminServer{
		cbClient: cbClient,
	}
}
