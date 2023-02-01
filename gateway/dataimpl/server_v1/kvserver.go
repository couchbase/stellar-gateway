package server_v1

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KvServer struct {
	kv_v1.UnimplementedKvServer

	cbClient *gocb.Cluster
}

func (s *KvServer) getCollection(
	ctx context.Context,
	bucketName, scopeName, collectionName string,
) *gocb.Collection {
	client := s.cbClient
	bucket := client.Bucket(bucketName)
	scope := bucket.Scope(scopeName)
	collection := scope.Collection(collectionName)
	return collection
}

func (s *KvServer) getResultToContent(
	ctx context.Context,
	result *gocb.GetResult,
) (psTranscodeData, *status.Status) {
	var contentData psTranscodeData
	err := result.Content(&contentData)
	if err != nil {
		// The transcoder that we implement in the gateway should not be possible
		// to lead to errors being produced here.  If they are produced, this is
		// considered a serious internal error, so we report that error and then
		// return an INTERNAL error to the client.

		log.Printf("Unexpected transcoder failure: %s", err)

		// TODO(brett19): We should conditionally attach debug information here to
		// indicate what the actual transcoding error was.
		return psTranscodeData{}, status.New(
			codes.Internal,
			"An unexpected internal transcoding failure occurred.")
	}

	return contentData, nil
}

func (s *KvServer) Get(ctx context.Context, in *kv_v1.GetRequest) (*kv_v1.GetResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.GetOptions
	opts.WithExpiry = true
	opts.Transcoder = customTranscoder{}
	opts.Context = ctx
	result, err := coll.Get(in.Key, &opts)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocbcore.ErrMemdAccessError) {
			// TODO(brett19): Need to confirm ErrMemdAccessError works here.
			return nil, newCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	contentData, errSt := s.getResultToContent(ctx, result)
	if errSt != nil {
		return nil, errSt.Err()
	}

	return &kv_v1.GetResponse{
		Content:     contentData.ContentBytes,
		ContentType: contentData.ContentType,
		Cas:         casFromGocb(result.Cas()),
		Expiry:      timeFromGo(result.ExpiryTime()),
	}, nil
}

func (s *KvServer) Insert(ctx context.Context, in *kv_v1.InsertRequest) (*kv_v1.InsertResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var contentData psTranscodeData
	contentData.ContentBytes = in.Content
	contentData.ContentType = in.ContentType

	var opts gocb.InsertOptions
	opts.Transcoder = customTranscoder{}
	opts.Context = ctx

	if in.Expiry != nil {
		opts.Expiry = time.Until(timeToGo(in.Expiry))
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*kv_v1.InsertRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*kv_v1.InsertRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelToGocb(levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.Insert(in.Key, contentData, &opts)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentExists) {
			return nil, newDocExistsStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocbcore.ErrMemdAccessError) {
			// TODO(brett19): Need to confirm ErrMemdAccessError works here.
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.InsertResponse{
		Cas:           casFromGocb(result.Cas()),
		MutationToken: tokenFromGocb(result.MutationToken()),
	}, nil
}

func (s *KvServer) Upsert(ctx context.Context, in *kv_v1.UpsertRequest) (*kv_v1.UpsertResponse, error) {
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
		opts.Expiry = time.Until(timeToGo(in.Expiry))
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*kv_v1.UpsertRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*kv_v1.UpsertRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelToGocb(levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.Upsert(in.Key, contentData, &opts)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentExists) {
			// TODO(brett19): Need to confirm this is the right error for CAS mismatch
			return nil, newDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocbcore.ErrMemdAccessError) {
			// TODO(brett19): Need to confirm ErrMemdAccessError works here.
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.UpsertResponse{
		Cas:           casFromGocb(result.Cas()),
		MutationToken: tokenFromGocb(result.MutationToken()),
	}, nil
}

func (s *KvServer) Replace(ctx context.Context, in *kv_v1.ReplaceRequest) (*kv_v1.ReplaceResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var contentData psTranscodeData
	contentData.ContentBytes = in.Content
	contentData.ContentType = in.ContentType

	var opts gocb.ReplaceOptions
	opts.Transcoder = customTranscoder{}
	opts.Context = ctx

	if in.Cas != nil {
		opts.Cas = casToGocb(in.Cas)
	}

	if in.Expiry == nil {
		opts.PreserveExpiry = true
	} else {
		opts.Expiry = time.Until(timeToGo(in.Expiry))
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*kv_v1.ReplaceRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*kv_v1.ReplaceRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelToGocb(levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.Replace(in.Key, contentData, &opts)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentExists) {
			// TODO(brett19): Need to confirm this is the right error for CAS mismatch
			return nil, newDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocbcore.ErrMemdAccessError) {
			// TODO(brett19): Need to confirm ErrMemdAccessError works here.
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.ReplaceResponse{
		Cas:           casFromGocb(result.Cas()),
		MutationToken: tokenFromGocb(result.MutationToken()),
	}, nil
}

func (s *KvServer) Remove(ctx context.Context, in *kv_v1.RemoveRequest) (*kv_v1.RemoveResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.RemoveOptions
	opts.Context = ctx

	if in.Cas != nil {
		opts.Cas = casToGocb(in.Cas)
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*kv_v1.RemoveRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*kv_v1.RemoveRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelToGocb(levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.Remove(in.Key, &opts)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentExists) {
			// TODO(brett19): Need to confirm this is the right error for CAS mismatch
			return nil, newDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocbcore.ErrMemdAccessError) {
			// TODO(brett19): Need to confirm ErrMemdAccessError works here.
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.RemoveResponse{
		Cas:           casFromGocb(result.Cas()),
		MutationToken: tokenFromGocb(result.MutationToken()),
	}, nil
}

func (s *KvServer) Increment(ctx context.Context, in *kv_v1.IncrementRequest) (*kv_v1.IncrementResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.IncrementOptions
	opts.Context = ctx
	opts.Delta = in.Delta

	if in.Expiry != nil {
		opts.Expiry = time.Until(timeToGo(in.Expiry))
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*kv_v1.IncrementRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*kv_v1.IncrementRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelToGocb(levelSpec.DurabilityLevel)
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
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentExists) {
			// TODO(brett19): Need to confirm this is the right error for CAS mismatch
			return nil, newDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocbcore.ErrMemdBadDelta) {
			// TODO(brett19): Need to confirm ErrMemdBadDelta works here.
			return nil, newDocNotNumericContentStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocbcore.ErrMemdAccessError) {
			// TODO(brett19): Need to confirm ErrMemdAccessError works here.
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.IncrementResponse{
		Cas:           casFromGocb(result.Cas()),
		Content:       int64(result.Content()),
		MutationToken: tokenFromGocb(result.MutationToken()),
	}, nil
}

func (s *KvServer) Decrement(ctx context.Context, in *kv_v1.DecrementRequest) (*kv_v1.DecrementResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.DecrementOptions
	opts.Context = ctx
	opts.Delta = in.Delta

	if in.Expiry != nil {
		opts.Expiry = time.Until(timeToGo(in.Expiry))
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*kv_v1.DecrementRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*kv_v1.DecrementRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelToGocb(levelSpec.DurabilityLevel)
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
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentExists) {
			// TODO(brett19): Need to confirm this is the right error for CAS mismatch
			return nil, newDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocbcore.ErrMemdBadDelta) {
			// TODO(brett19): Need to confirm ErrMemdBadDelta works here.
			return nil, newDocNotNumericContentStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocbcore.ErrMemdAccessError) {
			// TODO(brett19): Need to confirm ErrMemdAccessError works here.
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.DecrementResponse{
		Cas:           casFromGocb(result.Cas()),
		Content:       int64(result.Content()),
		MutationToken: tokenFromGocb(result.MutationToken()),
	}, nil
}

func (s *KvServer) Append(ctx context.Context, in *kv_v1.AppendRequest) (*kv_v1.AppendResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var contentData psTranscodeData
	contentData.ContentBytes = in.Content

	var opts gocb.AppendOptions
	opts.Context = ctx

	if in.Cas != nil {
		opts.Cas = casToGocb(in.Cas)
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*kv_v1.AppendRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*kv_v1.AppendRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelToGocb(levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.Binary().Append(in.Key, in.Content, &opts)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentExists) {
			// TODO(brett19): Need to confirm this is the right error for CAS mismatch
			return nil, newDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocbcore.ErrMemdAccessError) {
			// TODO(brett19): Need to confirm ErrMemdAccessError works here.
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.AppendResponse{
		Cas:           casFromGocb(result.Cas()),
		MutationToken: tokenFromGocb(result.MutationToken()),
	}, nil
}

func (s *KvServer) Prepend(ctx context.Context, in *kv_v1.PrependRequest) (*kv_v1.PrependResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var contentData psTranscodeData
	contentData.ContentBytes = in.Content

	var opts gocb.PrependOptions
	opts.Context = ctx

	if in.Cas != nil {
		opts.Cas = casToGocb(in.Cas)
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*kv_v1.PrependRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*kv_v1.PrependRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelToGocb(levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.Binary().Prepend(in.Key, in.Content, &opts)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentExists) {
			// TODO(brett19): Need to confirm this is the right error for CAS mismatch
			return nil, newDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocbcore.ErrMemdAccessError) {
			// TODO(brett19): Need to confirm ErrMemdAccessError works here.
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.PrependResponse{
		Cas:           casFromGocb(result.Cas()),
		MutationToken: tokenFromGocb(result.MutationToken()),
	}, nil
}

func (s *KvServer) LookupIn(ctx context.Context, in *kv_v1.LookupInRequest) (*kv_v1.LookupInResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.LookupInOptions
	opts.Context = ctx

	var specs []gocb.LookupInSpec
	for _, spec := range in.Specs {
		switch spec.Operation {
		case kv_v1.LookupInRequest_Spec_GET:
			specOpts := gocb.GetSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.GetSpec(spec.Path, &specOpts))
		case kv_v1.LookupInRequest_Spec_EXISTS:
			specOpts := gocb.ExistsSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.ExistsSpec(spec.Path, &specOpts))
		case kv_v1.LookupInRequest_Spec_COUNT:
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
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocbcore.ErrMemdAccessError) {
			// TODO(brett19): Need to confirm ErrMemdAccessError works here.
			return nil, newCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	var respSpecs []*kv_v1.LookupInResponse_Spec

	for specIdx := range specs {
		var contentBytes json.RawMessage
		err := result.ContentAt(uint(specIdx), &contentBytes)
		if err != nil {
			var errSt *status.Status
			if errors.Is(err, gocb.ErrPathNotFound) {
				errSt = newSdPathNotFoundStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[specIdx].Path)
			} else if errors.Is(err, gocb.ErrPathMismatch) {
				errSt = newSdPathMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[specIdx].Path)
			} else if errors.Is(err, gocb.ErrPathInvalid) {
				// Note that this is INVALID_ARGUMENTS not FAILED_PRECONDITION
				return nil, newSdPathEinvalStatus(err, in.Specs[specIdx].Path).Err()
			} else {
				// TODO(brett19): Need to implement remaining possible sub-document errors.
				errSt = newUnknownStatus(err)
			}

			respSpecs = append(respSpecs, &kv_v1.LookupInResponse_Spec{
				Status: errSt.Proto(),
			})
		} else {
			respSpecs = append(respSpecs, &kv_v1.LookupInResponse_Spec{
				Content: []byte(contentBytes),
			})
		}
	}

	return &kv_v1.LookupInResponse{
		Specs: respSpecs,
		Cas:   casFromGocb(result.Cas()),
	}, nil
}

func (s *KvServer) MutateIn(ctx context.Context, in *kv_v1.MutateInRequest) (*kv_v1.MutateInResponse, error) {
	coll := s.getCollection(ctx, in.BucketName, in.ScopeName, in.CollectionName)

	var opts gocb.MutateInOptions
	opts.Context = ctx

	var specs []gocb.MutateInSpec
	for _, spec := range in.Specs {
		switch spec.Operation {
		case kv_v1.MutateInRequest_Spec_INSERT:
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
		case kv_v1.MutateInRequest_Spec_UPSERT:
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
		case kv_v1.MutateInRequest_Spec_REPLACE:
			specOpts := gocb.ReplaceSpecOptions{}
			if spec.Flags != nil {
				if spec.Flags.Xattr != nil {
					specOpts.IsXattr = *spec.Flags.Xattr
				}
			}
			specs = append(specs, gocb.ReplaceSpec(spec.Path, json.RawMessage(spec.Content), &specOpts))
		case kv_v1.MutateInRequest_Spec_REMOVE:
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
		case kv_v1.MutateInRequest_Spec_ARRAY_APPEND:
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
		case kv_v1.MutateInRequest_Spec_ARRAY_PREPEND:
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
		case kv_v1.MutateInRequest_Spec_ARRAY_INSERT:
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
		case kv_v1.MutateInRequest_Spec_ARRAY_ADD_UNIQUE:
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
		case kv_v1.MutateInRequest_Spec_COUNTER:
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
				// TODO(brett19): Need to add additional context for which spec failed.
				return nil, status.New(codes.InvalidArgument, "Invalid counter delta specified.").Err()
			}
			specs = append(specs, gocb.IncrementSpec(spec.Path, count, &specOpts))
		}
	}

	if in.Cas != nil {
		opts.Cas = casToGocb(in.Cas)
	}

	if in.DurabilitySpec != nil {
		if legacySpec, ok := in.DurabilitySpec.(*kv_v1.MutateInRequest_LegacyDurabilitySpec); ok {
			opts.PersistTo = uint(legacySpec.LegacyDurabilitySpec.NumPersisted)
			opts.ReplicateTo = uint(legacySpec.LegacyDurabilitySpec.NumReplicated)
		}
		if levelSpec, ok := in.DurabilitySpec.(*kv_v1.MutateInRequest_DurabilityLevel); ok {
			dl, errSt := durabilityLevelToGocb(levelSpec.DurabilityLevel)
			if errSt != nil {
				return nil, errSt.Err()
			}
			opts.DurabilityLevel = dl
		}
	}

	result, err := coll.MutateIn(in.Key, specs, &opts)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentExists) {
			// TODO(brett19): Need to confirm this is the right error for CAS mismatch
			return nil, newDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocb.ErrDocumentLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, gocbcore.ErrMemdAccessError) {
			// TODO(brett19): Need to confirm ErrMemdAccessError works here.
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	var respSpecs []*kv_v1.MutateInResponse_Spec

	for specIdx := range specs {
		var contentBytes json.RawMessage
		err := result.ContentAt(uint(specIdx), &contentBytes)
		if err != nil {
			// An error occuring during a mutateIn operation causes the entire operation
			// to fail, so we just return an entire operation failure in that case.
			if errors.Is(err, gocb.ErrPathNotFound) {
				return nil, newSdPathNotFoundStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[specIdx].Path).Err()
			} else if errors.Is(err, gocb.ErrPathMismatch) {
				return nil, newSdPathMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[specIdx].Path).Err()
			} else if errors.Is(err, gocb.ErrPathInvalid) {
				// Note that this is INVALID_ARGUMENTS not FAILED_PRECONDITION
				return nil, newSdPathEinvalStatus(err, in.Specs[specIdx].Path).Err()
			}
			// TODO(brett19): Need to implement remaining possible sub-document errors.
			// Implementing the other errors is complicated because it does not appear that gocb
			// is properly exporting all the possible errors that can occur here...
			return nil, cbGenericErrToPsStatus(err).Err()
		}

		respSpecs = append(respSpecs, &kv_v1.MutateInResponse_Spec{
			Content: []byte(contentBytes),
		})
	}

	return &kv_v1.MutateInResponse{
		Specs:         respSpecs,
		Cas:           casFromGocb(result.Cas()),
		MutationToken: tokenFromGocb(result.MutationToken()),
	}, nil
}

func NewKvServer(cbClient *gocb.Cluster) *KvServer {
	return &KvServer{
		cbClient: cbClient,
	}
}
