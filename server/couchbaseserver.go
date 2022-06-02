package server

import (
	"context"
	"errors"
	"log"
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

func casFromPs(cas *protos.Cas) gocb.Cas {
	return gocb.Cas(cas.Value)
}

func timeToPs(when time.Time) *timestamppb.Timestamp {
	if when.IsZero() {
		return nil
	}
	return timestamppb.New(when)
}

func timeFromPs(ts *timestamppb.Timestamp) time.Time {
	return ts.AsTime()
}

func durabilityLevelFromPs(dl *protos.DurabilityLevel) (gocb.DurabilityLevel, *status.Status) {
	if dl == nil {
		return gocb.DurabilityLevelNone, nil
	}

	switch *dl {
	case protos.DurabilityLevel_MAJORITY:
		return gocb.DurabilityLevelMajority, nil
	case protos.DurabilityLevel_MAJORITY_AND_PERSIST_TO_ACTIVE:
		return gocb.DurabilityLevelMajorityAndPersistOnMaster, nil
	case protos.DurabilityLevel_PERSIST_TO_MAJORITY:
		return gocb.DurabilityLevelPersistToMajority, nil
	}

	return gocb.DurabilityLevelNone, status.New(codes.InvalidArgument, "invalid durability level options specified")
}

func cbErrToPs(err error) error {
	log.Printf("handling error: %+v", err)

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
		if errorDetails != nil {
			st, _ = st.WithDetails(errorDetails)
		}
		return st.Err()
	}

	// this never actually makes it back to the GRPC client, but we
	// do the translation here anyways...
	if errors.Is(err, context.Canceled) {
		return makeError(codes.Canceled, "request canceled")
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

	topologyManager *TopologyManager
	cbClient        *gocb.Cluster
}

func (s *couchbaseServer) getCollection(ctx context.Context, bucketName, scopeName, collectionName string) *gocb.Collection {
	client := s.cbClient
	bucket := client.Bucket(bucketName)
	scope := bucket.Scope(scopeName)
	collection := scope.Collection(collectionName)
	return collection
}

func (s *couchbaseServer) WatchRouting(in *protos.WatchRoutingRequest, out protos.Couchbase_WatchRoutingServer) error {
	// TODO(brett19): Implement proper topology updates...
	// For now we just fill out the entiry topology such that all the endpoints
	// point back to this singular node which can handle all request types.  Note
	// that we do not generate a vbucket map at the moment.

topologyLoop:
	for {
		topology := s.topologyManager.GetTopology()

		err := out.Send(&protos.WatchRoutingResponse{
			Endpoints: topology.Endpoints,
			KvRouting: &protos.WatchRoutingResponse_VbucketRouting{
				VbucketRouting: &protos.VbucketMapRouting{
					Endpoints: topology.Endpoints,
					Vbuckets:  nil,
				},
			},
			QueryRouting: &protos.QueryRouting{
				Endpoints: topology.Endpoints,
			},
			SearchQueryRouting: &protos.SearchQueryRouting{
				Endpoints: topology.Endpoints,
			},
			AnalyticsQueryRouting: &protos.AnalyticsQueryRouting{
				Endpoints: topology.Endpoints,
			},
		})
		if err != nil {
			log.Printf("failed to send topology update: %s", err)
		}

		select {
		case <-time.After(15 * time.Second):
			// we send toplogy updates every 15 seconds for demo purposes
		case <-out.Context().Done():
			break topologyLoop
		}
	}

	return nil
}

func (s *couchbaseServer) Get(ctx context.Context, in *protos.GetRequest) (*protos.GetResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.GetOptions
	opts.WithExpiry = true
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
		Expiry:      timeToPs(result.ExpiryTime()),
	}, nil
}

func (s *couchbaseServer) Insert(ctx context.Context, in *protos.InsertRequest) (*protos.InsertResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var contentData psTranscodeData
	contentData.ContentBytes = in.Content
	contentData.ContentType = in.ContentType

	var opts gocb.InsertOptions
	opts.Transcoder = customTranscoder{}
	opts.Context = ctx

	if in.Expiry != nil {
		opts.Expiry = time.Until(timeFromPs(in.Expiry))
	}

	dl, errSt := durabilityLevelFromPs(in.DurabilityLevel)
	if errSt != nil {
		return nil, errSt.Err()
	}
	opts.DurabilityLevel = dl

	result, err := coll.Insert(in.Key, contentData, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &protos.InsertResponse{
		Cas: casToPs(result.Cas()),
	}, nil
}

func (s *couchbaseServer) Upsert(ctx context.Context, in *protos.UpsertRequest) (*protos.UpsertResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var contentData psTranscodeData
	contentData.ContentBytes = in.Content
	contentData.ContentType = in.ContentType

	var opts gocb.UpsertOptions
	opts.Transcoder = customTranscoder{}
	opts.Context = ctx

	if in.Expiry == nil {
		opts.PreserveExpiry = true
	} else {
		opts.Expiry = time.Until(timeFromPs(in.Expiry))
	}

	dl, errSt := durabilityLevelFromPs(in.DurabilityLevel)
	if errSt != nil {
		return nil, errSt.Err()
	}
	opts.DurabilityLevel = dl

	result, err := coll.Upsert(in.Key, contentData, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &protos.UpsertResponse{
		Cas: casToPs(result.Cas()),
	}, nil
}

func (s *couchbaseServer) Replace(ctx context.Context, in *protos.ReplaceRequest) (*protos.ReplaceResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var contentData psTranscodeData
	contentData.ContentBytes = in.Content
	contentData.ContentType = in.ContentType

	var opts gocb.ReplaceOptions
	opts.Transcoder = customTranscoder{}
	opts.Context = ctx

	if in.Cas != nil {
		opts.Cas = casFromPs(in.Cas)
	}

	if in.Expiry == nil {
		opts.PreserveExpiry = true
	} else {
		opts.Expiry = time.Until(timeFromPs(in.Expiry))
	}

	dl, errSt := durabilityLevelFromPs(in.DurabilityLevel)
	if errSt != nil {
		return nil, errSt.Err()
	}
	opts.DurabilityLevel = dl

	result, err := coll.Replace(in.Key, contentData, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &protos.ReplaceResponse{
		Cas: casToPs(result.Cas()),
	}, nil
}

func (s *couchbaseServer) Remove(ctx context.Context, in *protos.RemoveRequest) (*protos.RemoveResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.RemoveOptions
	opts.Context = ctx

	if in.Cas != nil {
		opts.Cas = casFromPs(in.Cas)
	}

	dl, errSt := durabilityLevelFromPs(in.DurabilityLevel)
	if errSt != nil {
		return nil, errSt.Err()
	}
	opts.DurabilityLevel = dl

	result, err := coll.Remove(in.Key, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &protos.RemoveResponse{
		Cas: casToPs(result.Cas()),
	}, nil
}

func NewCouchbaseServer(topologyManager *TopologyManager, cbClient *gocb.Cluster) *couchbaseServer {
	return &couchbaseServer{
		topologyManager: topologyManager,
		cbClient:        cbClient,
	}
}
