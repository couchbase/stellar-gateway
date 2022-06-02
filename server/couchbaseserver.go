package server

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/stellar-nebula/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/runtime/protoiface"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func casToPs(cas gocb.Cas) *protos.Cas {
	return &protos.Cas{
		Value: uint64(cas),
	}
}

func durationToPs(when time.Time) *timestamppb.Timestamp {
	return timestamppb.New(when)
}

func cbErrToPs(err error) error {
	var errorDetails protoiface.MessageV1

	var keyValueContext *gocb.KeyValueError
	if errors.As(err, &keyValueContext) {
		// TODO(bret19): Need to include more error context here
		errorDetails = &protos.ErrorInfo{
			Reason: keyValueContext.ErrorName,
			Metadata: map[string]string{
				"bucket":     keyValueContext.BucketName,
				"scope":      keyValueContext.ScopeName,
				"collection": keyValueContext.CollectionName,
				"key":        keyValueContext.DocumentID,
			},
		}
	}

	makeError := func(c codes.Code, msg string) error {
		st := status.New(c, msg)
		st, _ = st.WithDetails(errorDetails)
		return st.Err()
	}

	// TODO(brett19): Need to provide translation for more errors
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return makeError(codes.NotFound, "document not found")
	} else if errors.Is(err, gocb.ErrDocumentExists) {
		return makeError(codes.AlreadyExists, "document already exists")
	}

	return makeError(codes.Internal, "an unknown error occurred")
}

type couchbaseServer struct {
	protos.UnimplementedCouchbaseServer

	cbClient *gocb.Cluster
}

func (s *couchbaseServer) getCollection(ctx context.Context, bucketName, scopeName, collectionName string) *gocb.Collection {
	client := s.cbClient
	bucket := client.Bucket(bucketName)
	scope := bucket.Scope(scopeName)
	collection := scope.Collection(collectionName)
	return collection
}

func (s *couchbaseServer) Get(ctx context.Context, in *protos.GetRequest) (*protos.GetResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.GetOptions
	opts.WithExpiry = true
	//opts.Internal.User = "someone"
	opts.Transcoder = customTranscoder{}
	opts.Context = ctx
	result, err := coll.Get(in.Key, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	var contentData psTranscodeData
	err = result.Content(&contentData)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &protos.GetResponse{
		Content:     contentData.ContentBytes,
		ContentType: contentData.ContentType,
		Cas:         casToPs(result.Cas()),
		Expiry:      durationToPs(result.ExpiryTime()),
	}, nil
}

func (s *couchbaseServer) Upsert(ctx context.Context, in *protos.UpsertRequest) (*protos.UpsertResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var contentData psTranscodeData
	contentData.ContentBytes = in.Content
	contentData.ContentType = in.ContentType

	var opts gocb.UpsertOptions
	//opts.Internal.User = "someone"
	opts.Transcoder = customTranscoder{}
	opts.Context = ctx
	result, err := coll.Upsert(in.Key, contentData, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &protos.UpsertResponse{
		Cas: casToPs(result.Cas()),
	}, nil
}

func NewCouchbaseServer(cbClient *gocb.Cluster) *couchbaseServer {
	return &couchbaseServer{
		cbClient: cbClient,
	}
}
