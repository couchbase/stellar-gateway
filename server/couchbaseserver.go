package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/stellar-nebula/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

func (s *couchbaseServer) Hello(ctx context.Context, in *protos.HelloRequest) (*protos.HelloResponse, error) {
	return &protos.HelloResponse{}, nil
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

func (s *couchbaseServer) LookupIn(ctx context.Context, in *protos.LookupInRequest) (*protos.LookupInResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.LookupInOptions
	opts.Context = ctx

	var specs []gocb.LookupInSpec
	for _, spec := range in.Specs {
		switch spec.Operation {
		case protos.LookupInRequest_Spec_GET:
			specOpts := gocb.GetSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.GetSpec(spec.Path, &specOpts))
		case protos.LookupInRequest_Spec_EXISTS:
			specOpts := gocb.ExistsSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.ExistsSpec(spec.Path, &specOpts))
		case protos.LookupInRequest_Spec_COUNT:
			specOpts := gocb.CountSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.CountSpec(spec.Path, &specOpts))
		}
	}

	result, err := coll.LookupIn(in.Key, specs, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	var respSpecs []*protos.LookupInResponse_Spec

	for specIdx := range specs {
		var contentBytes json.RawMessage
		err := result.ContentAt(uint(specIdx), &contentBytes)
		if err != nil {
			respSpecs = append(respSpecs, &protos.LookupInResponse_Spec{
				// Status:  cbErrToPsStatus(err),
				Content: nil,
			})
			continue
		}

		respSpecs = append(respSpecs, &protos.LookupInResponse_Spec{
			Content: []byte(contentBytes),
		})
	}

	return &protos.LookupInResponse{
		Specs: respSpecs,
		Cas:   casToPs(result.Cas()),
	}, nil
}

func (s *couchbaseServer) MutateIn(ctx context.Context, in *protos.MutateInRequest) (*protos.MutateInResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.MutateInOptions
	opts.Context = ctx

	var specs []gocb.MutateInSpec
	for _, spec := range in.Specs {
		switch spec.Operation {
		case protos.MutateInRequest_Spec_INSERT:
			specOpts := gocb.InsertSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.CreatePath != nil {
					specOpts.CreatePath = *spec.Flags.CreatePath
				}
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.InsertSpec(spec.Path, json.RawMessage(spec.Content), &specOpts))
		case protos.MutateInRequest_Spec_UPSERT:
			specOpts := gocb.UpsertSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.CreatePath != nil {
					specOpts.CreatePath = *spec.Flags.CreatePath
				}
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.UpsertSpec(spec.Path, json.RawMessage(spec.Content), &specOpts))
		case protos.MutateInRequest_Spec_REPLACE:
			specOpts := gocb.ReplaceSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.ReplaceSpec(spec.Path, json.RawMessage(spec.Content), &specOpts))
		case protos.MutateInRequest_Spec_REMOVE:
			if spec.Content != nil {
				return nil, status.New(codes.InvalidArgument, "cannot specify content for remove spec").Err()
			}

			specOpts := gocb.RemoveSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.RemoveSpec(spec.Path, &specOpts))
		}
	}

	if in.Cas != nil {
		opts.Cas = casFromPs(in.Cas)
	}

	dl, errSt := durabilityLevelFromPs(in.DurabilityLevel)
	if errSt != nil {
		return nil, errSt.Err()
	}
	opts.DurabilityLevel = dl

	result, err := coll.MutateIn(in.Key, specs, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	var respSpecs []*protos.MutateInResponse_Spec

	for specIdx := range specs {
		var contentBytes json.RawMessage
		err := result.ContentAt(uint(specIdx), &contentBytes)
		if err != nil {
			// if we get an error, we just put nil bytes
			// TODO(brett19): check if we need to handle mutatein spec errors
			respSpecs = append(respSpecs, &protos.MutateInResponse_Spec{
				Content: nil,
			})
			continue
		}

		respSpecs = append(respSpecs, &protos.MutateInResponse_Spec{
			Content: []byte(contentBytes),
		})
	}

	return &protos.MutateInResponse{
		Specs: respSpecs,
		Cas:   casToPs(result.Cas()),
	}, nil
}

func (s *couchbaseServer) Query(in *protos.QueryRequest, out protos.Couchbase_QueryServer) error {
	var opts gocb.QueryOptions

	// metrics are included by default
	opts.Metrics = true

	if in.ReadOnly != nil {
		opts.Readonly = *in.ReadOnly
	}

	if in.Prepared != nil {
		opts.Adhoc = !*in.Prepared
	}

	if in.TuningOptions != nil {
		if in.TuningOptions.MaxParallelism != nil {
			opts.MaxParallelism = *in.TuningOptions.MaxParallelism
		}
		if in.TuningOptions.PipelineBatch != nil {
			opts.PipelineBatch = *in.TuningOptions.PipelineBatch
		}
		if in.TuningOptions.PipelineCap != nil {
			opts.PipelineCap = *in.TuningOptions.PipelineCap
		}
		if in.TuningOptions.ScanWait != nil {
			opts.ScanWait = durationFromPs(in.TuningOptions.ScanWait)
		}
		if in.TuningOptions.ScanCap != nil {
			opts.ScanCap = *in.TuningOptions.ScanCap
		}
		if in.TuningOptions.DisableMetrics != nil {
			opts.Metrics = !*in.TuningOptions.DisableMetrics
		}
	}

	if in.ClientContextId != nil {
		opts.ClientContextID = *in.ClientContextId
	}

	result, err := s.cbClient.Query(in.Statement, &opts)
	if err != nil {
		return cbErrToPs(err)
	}

	var rowCache [][]byte
	var rowCacheNumBytes int = 0
	const MAX_ROW_BYTES = 1024

	for result.Next() {
		var rowBytes json.RawMessage
		result.Row(&rowBytes)
		rowNumBytes := len(rowBytes)

		if rowCacheNumBytes+rowNumBytes > MAX_ROW_BYTES {
			// adding this row to the cache would exceed its maximum number of
			// bytes, so we need to evict all these rows...
			out.Send(&protos.QueryResponse{
				Rows:     rowCache,
				MetaData: nil,
			})
			rowCache = nil
			rowCacheNumBytes = 0
		}

		rowCache = append(rowCache, rowBytes)
		rowCacheNumBytes += rowNumBytes
	}

	var psMetaData *protos.QueryResponse_MetaData

	metaData, err := result.MetaData()
	if err == nil {
		var psMetrics *protos.QueryResponse_MetaData_Metrics
		if opts.Metrics {
			psMetrics = &protos.QueryResponse_MetaData_Metrics{
				ElapsedTime:   durationToPs(metaData.Metrics.ElapsedTime),
				ExecutionTime: durationToPs(metaData.Metrics.ExecutionTime),
				ResultCount:   metaData.Metrics.ResultCount,
				ResultsSize:   metaData.Metrics.ResultSize,
				MutationCount: metaData.Metrics.MutationCount,
				SortCount:     metaData.Metrics.SortCount,
				ErrorCount:    metaData.Metrics.ErrorCount,
				WarningCount:  metaData.Metrics.WarningCount,
			}
		}

		psMetaData = &protos.QueryResponse_MetaData{
			RequestId:       metaData.RequestID,
			ClientContextId: metaData.ClientContextID,
			Metrics:         psMetrics,
		}
	}

	// if we have any rows or meta-data left to stream, we send that first
	// before we process any errors that occurred.
	if rowCache != nil || psMetaData != nil {
		out.Send(&protos.QueryResponse{
			Rows:     rowCache,
			MetaData: psMetaData,
		})

		rowCache = nil
		rowCacheNumBytes = 0
		psMetaData = nil
	}

	err = result.Err()
	if err != nil {
		return cbErrToPs(err)
	}

	return nil
}

func NewCouchbaseServer(cbClient *gocb.Cluster) *couchbaseServer {
	return &couchbaseServer{
		cbClient: cbClient,
	}
}
