package server_v1

import (
	"context"
	"encoding/json"
	"time"

	"github.com/couchbase/gocb/v2"
	data_v1 "github.com/couchbase/stellar-nebula/genproto/data/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DataServer struct {
	data_v1.UnimplementedDataServer

	cbClient *gocb.Cluster
}

func (s *DataServer) getCollection(ctx context.Context, bucketName, scopeName, collectionName string) *gocb.Collection {
	client := s.cbClient
	bucket := client.Bucket(bucketName)
	scope := bucket.Scope(scopeName)
	collection := scope.Collection(collectionName)
	return collection
}

func (s *DataServer) Get(ctx context.Context, in *data_v1.GetRequest) (*data_v1.GetResponse, error) {
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

	return &data_v1.GetResponse{
		Content:     contentData.ContentBytes,
		ContentType: contentData.ContentType,
		Cas:         casToPs(result.Cas()),
		Expiry:      timeToPs(result.ExpiryTime()),
	}, nil
}

func (s *DataServer) Insert(ctx context.Context, in *data_v1.InsertRequest) (*data_v1.InsertResponse, error) {
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

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*data_v1.InsertRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*data_v1.InsertRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelFromPs(&levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.Insert(in.Key, contentData, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &data_v1.InsertResponse{
		Cas:           casToPs(result.Cas()),
		MutationToken: tokenToPs(result.MutationToken()),
	}, nil
}

func (s *DataServer) Upsert(ctx context.Context, in *data_v1.UpsertRequest) (*data_v1.UpsertResponse, error) {
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

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*data_v1.UpsertRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*data_v1.UpsertRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelFromPs(&levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.Upsert(in.Key, contentData, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &data_v1.UpsertResponse{
		Cas:           casToPs(result.Cas()),
		MutationToken: tokenToPs(result.MutationToken()),
	}, nil
}

func (s *DataServer) Replace(ctx context.Context, in *data_v1.ReplaceRequest) (*data_v1.ReplaceResponse, error) {
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

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*data_v1.ReplaceRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*data_v1.ReplaceRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelFromPs(&levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.Replace(in.Key, contentData, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &data_v1.ReplaceResponse{
		Cas:           casToPs(result.Cas()),
		MutationToken: tokenToPs(result.MutationToken()),
	}, nil
}

func (s *DataServer) Remove(ctx context.Context, in *data_v1.RemoveRequest) (*data_v1.RemoveResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.RemoveOptions
	opts.Context = ctx

	if in.Cas != nil {
		opts.Cas = casFromPs(in.Cas)
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*data_v1.RemoveRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*data_v1.RemoveRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelFromPs(&levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.Remove(in.Key, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &data_v1.RemoveResponse{
		Cas:           casToPs(result.Cas()),
		MutationToken: tokenToPs(result.MutationToken()),
	}, nil
}

func (s *DataServer) Increment(ctx context.Context, in *data_v1.IncrementRequest) (*data_v1.IncrementResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.IncrementOptions
	opts.Context = ctx
	opts.Delta = in.Delta

	if in.Expiry != nil {
		opts.Expiry = time.Until(timeFromPs(in.Expiry))
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*data_v1.IncrementRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*data_v1.IncrementRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelFromPs(&levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	if in.Initial != nil {
		opts.Initial = *in.Initial
	}

	result, err := coll.Binary().Increment(in.Key, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &data_v1.IncrementResponse{
		Cas:           casToPs(result.Cas()),
		Content:       int64(result.Content()),
		MutationToken: tokenToPs(result.MutationToken()),
	}, nil
}

func (s *DataServer) Decrement(ctx context.Context, in *data_v1.DecrementRequest) (*data_v1.DecrementResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.DecrementOptions
	opts.Context = ctx
	opts.Delta = in.Delta

	if in.Expiry != nil {
		opts.Expiry = time.Until(timeFromPs(in.Expiry))
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*data_v1.DecrementRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*data_v1.DecrementRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelFromPs(&levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	if in.Initial != nil {
		opts.Initial = *in.Initial
	}

	result, err := coll.Binary().Decrement(in.Key, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &data_v1.DecrementResponse{
		Cas:           casToPs(result.Cas()),
		Content:       int64(result.Content()),
		MutationToken: tokenToPs(result.MutationToken()),
	}, nil
}

func (s *DataServer) Append(ctx context.Context, in *data_v1.AppendRequest) (*data_v1.AppendResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var contentData psTranscodeData
	contentData.ContentBytes = in.Content

	var opts gocb.AppendOptions
	opts.Context = ctx

	if in.Cas != nil {
		opts.Cas = casFromPs(in.Cas)
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*data_v1.AppendRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*data_v1.AppendRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelFromPs(&levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.Binary().Append(in.Key, in.Content, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &data_v1.AppendResponse{
		Cas:           casToPs(result.Cas()),
		MutationToken: tokenToPs(result.MutationToken()),
	}, nil
}

func (s *DataServer) Prepend(ctx context.Context, in *data_v1.PrependRequest) (*data_v1.PrependResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var contentData psTranscodeData
	contentData.ContentBytes = in.Content

	var opts gocb.PrependOptions
	opts.Context = ctx

	if in.Cas != nil {
		opts.Cas = casFromPs(in.Cas)
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*data_v1.PrependRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*data_v1.PrependRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelFromPs(&levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.Binary().Prepend(in.Key, in.Content, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	return &data_v1.PrependResponse{
		Cas:           casToPs(result.Cas()),
		MutationToken: tokenToPs(result.MutationToken()),
	}, nil
}

func (s *DataServer) LookupIn(ctx context.Context, in *data_v1.LookupInRequest) (*data_v1.LookupInResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.LookupInOptions
	opts.Context = ctx

	var specs []gocb.LookupInSpec
	for _, spec := range in.Specs {
		switch spec.Operation {
		case data_v1.LookupInRequest_Spec_GET:
			specOpts := gocb.GetSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.GetSpec(spec.Path, &specOpts))
		case data_v1.LookupInRequest_Spec_EXISTS:
			specOpts := gocb.ExistsSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.ExistsSpec(spec.Path, &specOpts))
		case data_v1.LookupInRequest_Spec_COUNT:
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

	var respSpecs []*data_v1.LookupInResponse_Spec

	for specIdx := range specs {
		var contentBytes json.RawMessage
		err := result.ContentAt(uint(specIdx), &contentBytes)
		if err != nil {
			respSpecs = append(respSpecs, &data_v1.LookupInResponse_Spec{
				// Status:  cbErrToPsStatus(err),
				Content: nil,
			})
			continue
		}

		respSpecs = append(respSpecs, &data_v1.LookupInResponse_Spec{
			Content: []byte(contentBytes),
		})
	}

	return &data_v1.LookupInResponse{
		Specs: respSpecs,
		Cas:   casToPs(result.Cas()),
	}, nil
}

func (s *DataServer) MutateIn(ctx context.Context, in *data_v1.MutateInRequest) (*data_v1.MutateInResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.MutateInOptions
	opts.Context = ctx

	var specs []gocb.MutateInSpec
	for _, spec := range in.Specs {
		switch spec.Operation {
		case data_v1.MutateInRequest_Spec_INSERT:
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
		case data_v1.MutateInRequest_Spec_UPSERT:
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
		case data_v1.MutateInRequest_Spec_REPLACE:
			specOpts := gocb.ReplaceSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.ReplaceSpec(spec.Path, json.RawMessage(spec.Content), &specOpts))
		case data_v1.MutateInRequest_Spec_REMOVE:
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
		case data_v1.MutateInRequest_Spec_ARRAY_APPEND:
			specOpts := gocb.ArrayAppendSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.CreatePath != nil {
					specOpts.CreatePath = *spec.Flags.CreatePath
				}
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.ArrayAppendSpec(spec.Path, json.RawMessage(spec.Content), &specOpts))
		case data_v1.MutateInRequest_Spec_ARRAY_PREPEND:
			specOpts := gocb.ArrayPrependSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.CreatePath != nil {
					specOpts.CreatePath = *spec.Flags.CreatePath
				}
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.ArrayPrependSpec(spec.Path, json.RawMessage(spec.Content), &specOpts))
		case data_v1.MutateInRequest_Spec_ARRAY_INSERT:
			specOpts := gocb.ArrayInsertSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.CreatePath != nil {
					specOpts.CreatePath = *spec.Flags.CreatePath
				}
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.ArrayInsertSpec(spec.Path, json.RawMessage(spec.Content), &specOpts))
		case data_v1.MutateInRequest_Spec_ARRAY_ADD_UNIQUE:
			specOpts := gocb.ArrayAddUniqueSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.CreatePath != nil {
					specOpts.CreatePath = *spec.Flags.CreatePath
				}
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.ArrayAddUniqueSpec(spec.Path, json.RawMessage(spec.Content), &specOpts))
		case data_v1.MutateInRequest_Spec_COUNTER:
			specOpts := gocb.CounterSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.CreatePath != nil {
					specOpts.CreatePath = *spec.Flags.CreatePath
				}
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}

			var count int64
			if err := json.Unmarshal(spec.Content, &count); err != nil {
				return nil, cbErrToPs(err)
			}
			specs = append(specs, gocb.IncrementSpec(spec.Path, count, &specOpts))
		}
	}

	if in.Cas != nil {
		opts.Cas = casFromPs(in.Cas)
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*data_v1.MutateInRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*data_v1.MutateInRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelFromPs(&levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.MutateIn(in.Key, specs, &opts)
	if err != nil {
		return nil, cbErrToPs(err)
	}

	var respSpecs []*data_v1.MutateInResponse_Spec

	for specIdx := range specs {
		var contentBytes json.RawMessage
		err := result.ContentAt(uint(specIdx), &contentBytes)
		if err != nil {
			// if we get an error, we just put nil bytes
			// TODO(brett19): check if we need to handle mutatein spec errors
			respSpecs = append(respSpecs, &data_v1.MutateInResponse_Spec{
				Content: nil,
			})
			continue
		}

		respSpecs = append(respSpecs, &data_v1.MutateInResponse_Spec{
			Content: []byte(contentBytes),
		})
	}

	return &data_v1.MutateInResponse{
		Specs:         respSpecs,
		Cas:           casToPs(result.Cas()),
		MutationToken: tokenToPs(result.MutationToken()),
	}, nil
}

func NewDataServer(cbClient *gocb.Cluster) *DataServer {
	return &DataServer{
		cbClient: cbClient,
	}
}
