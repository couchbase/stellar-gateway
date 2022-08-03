package server

import (
	"context"

	"github.com/couchbase/gocb/v2"
	admin_bucket_v1 "github.com/couchbase/stellar-nebula/genproto/admin/bucket/v1"
)

type bucketAdminServer struct {
	admin_bucket_v1.UnimplementedBucketAdminServer

	cbClient *gocb.Cluster
}

func (s *bucketAdminServer) ListCollections(context context.Context, in *admin_bucket_v1.ListCollectionsRequest) (*admin_bucket_v1.ListCollectionsResponse, error) {
	collMgr := s.cbClient.Bucket(in.BucketName).Collections()

	result, err := collMgr.GetAllScopes(&gocb.GetAllScopesOptions{
		Context: context,
	})
	if err != nil {
		return nil, cbErrToPs(err)
	}

	var scopes []*admin_bucket_v1.ListCollectionsResponse_Scope
	for _, scope := range result {
		var collections []*admin_bucket_v1.ListCollectionsResponse_Collection

		for _, collection := range scope.Collections {
			collections = append(collections, &admin_bucket_v1.ListCollectionsResponse_Collection{
				Name: collection.Name,
			})
		}

		scopes = append(scopes, &admin_bucket_v1.ListCollectionsResponse_Scope{
			Name:        scope.Name,
			Collections: collections,
		})
	}

	return &admin_bucket_v1.ListCollectionsResponse{
		Scopes: scopes,
	}, nil
}

func (s *bucketAdminServer) CreateScope(context context.Context, in *admin_bucket_v1.CreateScopeRequest) (*admin_bucket_v1.CreateScopeResponse, error) {
	collMgr := s.cbClient.Bucket(in.BucketName).Collections()

	err := collMgr.CreateScope(in.ScopeName, &gocb.CreateScopeOptions{
		Context: context,
	})
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &admin_bucket_v1.CreateScopeResponse{}, nil
}

func (s *bucketAdminServer) DeleteScope(context context.Context, in *admin_bucket_v1.DeleteScopeRequest) (*admin_bucket_v1.DeleteScopeResponse, error) {
	collMgr := s.cbClient.Bucket(in.BucketName).Collections()

	err := collMgr.DropScope(in.ScopeName, &gocb.DropScopeOptions{
		Context: context,
	})
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &admin_bucket_v1.DeleteScopeResponse{}, nil
}

func (s *bucketAdminServer) CreateCollection(context context.Context, in *admin_bucket_v1.CreateCollectionRequest) (*admin_bucket_v1.CreateCollectionResponse, error) {
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

	return &admin_bucket_v1.CreateCollectionResponse{}, nil
}

func (s *bucketAdminServer) DeleteCollection(context context.Context, in *admin_bucket_v1.DeleteCollectionRequest) (*admin_bucket_v1.DeleteCollectionResponse, error) {
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

	return &admin_bucket_v1.DeleteCollectionResponse{}, nil
}

func NewBucketAdminServer(cbClient *gocb.Cluster) *bucketAdminServer {
	return &bucketAdminServer{
		cbClient: cbClient,
	}
}
