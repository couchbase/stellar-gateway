package server_v1

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KvServer struct {
	kv_v1.UnimplementedKvServer

	cbClient *gocbcorex.AgentManager
}

func (s *KvServer) getBucketAgent(
	ctx context.Context, bucketName string,
) (*gocbcorex.Agent, *status.Status) {
	bucketAgent, err := s.cbClient.GetBucketAgent(ctx, bucketName)
	if err != nil {
		return nil, cbGenericErrToPsStatus(err)
	}
	return bucketAgent, nil
}

func (s *KvServer) parseContent(
	bytes []byte, flags uint32,
) ([]byte, kv_v1.DocumentContentType, *status.Status) {
	valueType, compression := gocbcore.DecodeCommonFlags(flags)

	// Make sure compression is disabled
	if compression != gocbcore.NoCompression {
		return nil, 0, status.New(
			codes.Internal,
			"An unexpected compression was specified by the document.")
	}

	if valueType == gocbcore.BinaryType {
		return bytes, kv_v1.DocumentContentType_BINARY, nil
	} else if valueType == gocbcore.StringType {
		// we don't support string data types in this case and instead just
		// handle them as raw binary data instead...
		// TODO(brett19): Decide how to handle string transcoding better.
		return bytes, kv_v1.DocumentContentType_BINARY, nil
	} else if valueType == gocbcore.JSONType {
		return bytes, kv_v1.DocumentContentType_JSON, nil
	}

	return nil, 0, status.New(
		codes.Internal,
		"An unexpected value type was specified by the document.")
}

func (s *KvServer) encodeContent(
	contentBytes []byte, contentType kv_v1.DocumentContentType,
) ([]byte, uint32, *status.Status) {
	if contentType == kv_v1.DocumentContentType_BINARY {
		return contentBytes, gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression), nil
	} else if contentType == kv_v1.DocumentContentType_JSON {
		return contentBytes, gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.NoCompression), nil
	}

	return nil, 0, status.New(
		codes.InvalidArgument,
		"An unexpected content type was specified in the request.")
}

func (s *KvServer) Get(ctx context.Context, in *kv_v1.GetRequest) (*kv_v1.GetResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	result, err := bucketAgent.LookupIn(ctx, &gocbcorex.LookupInOptions{
		Key:            []byte(in.Key),
		ScopeName:      in.ScopeName,
		CollectionName: in.CollectionName,
		Ops: []memdx.LookupInOp{
			{
				Op:    memdx.LookupInOpTypeGet,
				Flags: memdx.SubdocOpFlagXattrPath,
				Path:  []byte("$document.exptime"),
			},
			{
				Op:    memdx.LookupInOpTypeGet,
				Flags: memdx.SubdocOpFlagXattrPath,
				Path:  []byte("$document.flags"),
			},
			{
				Op:    memdx.LookupInOpTypeGetDoc,
				Flags: memdx.SubdocOpFlagNone,
				Path:  nil,
			},
		},
	})
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	expiryTimeSecs, err := strconv.ParseInt(string(result.Ops[0].Value), 10, 64)
	if err != nil {
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	flags, err := strconv.ParseUint(string(result.Ops[1].Value), 10, 64)
	if err != nil {
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	expiryTime := time.Unix(expiryTimeSecs, 0)
	docValue := result.Ops[2].Value

	contentBytes, contentType, errSt := s.parseContent(docValue, uint32(flags))
	if errSt != nil {
		return nil, errSt.Err()
	}

	return &kv_v1.GetResponse{
		Content:     contentBytes,
		ContentType: contentType,
		Cas:         result.Cas,
		Expiry:      timeFromGo(expiryTime),
	}, nil
}

func (s *KvServer) Insert(ctx context.Context, in *kv_v1.InsertRequest) (*kv_v1.InsertResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	bytes, flags, errSt := s.encodeContent(in.Content, in.ContentType)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.AddOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Value = bytes
	opts.Flags = flags

	if in.Expiry != nil {
		if expirySpec, ok := in.Expiry.(*kv_v1.InsertRequest_ExpiryTime); ok {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
		} else if expirySpec, ok := in.Expiry.(*kv_v1.InsertRequest_ExpirySecs); ok {
			opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
		} else {
			return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
		}
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Add(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocExists) {
			return nil, newDocExistsStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.InsertResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Upsert(ctx context.Context, in *kv_v1.UpsertRequest) (*kv_v1.UpsertResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	bytes, flags, errSt := s.encodeContent(in.Content, in.ContentType)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.UpsertOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Value = bytes
	opts.Flags = flags

	if in.Expiry == nil {
		opts.PreserveExpiry = true
	} else if in.Expiry != nil {
		if expirySpec, ok := in.Expiry.(*kv_v1.UpsertRequest_ExpiryTime); ok {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
		} else if expirySpec, ok := in.Expiry.(*kv_v1.UpsertRequest_ExpirySecs); ok {
			opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
		} else {
			return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
		}
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Upsert(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.UpsertResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Replace(ctx context.Context, in *kv_v1.ReplaceRequest) (*kv_v1.ReplaceResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	bytes, flags, errSt := s.encodeContent(in.Content, in.ContentType)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.ReplaceOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Value = bytes
	opts.Flags = flags

	if in.Cas != nil {
		opts.Cas = *in.Cas
	}

	if in.Expiry == nil {
		opts.PreserveExpiry = true
	} else if in.Expiry != nil {
		if expirySpec, ok := in.Expiry.(*kv_v1.ReplaceRequest_ExpiryTime); ok {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
		} else if expirySpec, ok := in.Expiry.(*kv_v1.ReplaceRequest_ExpirySecs); ok {
			opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
		} else {
			return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
		}
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Replace(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, newDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.ReplaceResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Remove(ctx context.Context, in *kv_v1.RemoveRequest) (*kv_v1.RemoveResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.DeleteOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)

	if in.Cas != nil {
		opts.Cas = *in.Cas
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Delete(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, newDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.RemoveResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Increment(ctx context.Context, in *kv_v1.IncrementRequest) (*kv_v1.IncrementResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.IncrementOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Delta = in.Delta

	if in.Expiry != nil {
		if expirySpec, ok := in.Expiry.(*kv_v1.IncrementRequest_ExpiryTime); ok {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
		} else if expirySpec, ok := in.Expiry.(*kv_v1.IncrementRequest_ExpirySecs); ok {
			opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
		} else {
			return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
		}
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	if in.Initial != nil {
		opts.Initial = uint64(*in.Initial)
	}

	result, err := bucketAgent.Increment(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, newDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.IncrementResponse{
		Cas:           result.Cas,
		Content:       int64(result.Value),
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Decrement(ctx context.Context, in *kv_v1.DecrementRequest) (*kv_v1.DecrementResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.DecrementOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Delta = in.Delta

	if in.Expiry != nil {
		if expirySpec, ok := in.Expiry.(*kv_v1.DecrementRequest_ExpiryTime); ok {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
		} else if expirySpec, ok := in.Expiry.(*kv_v1.DecrementRequest_ExpirySecs); ok {
			opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
		} else {
			return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
		}
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	if in.Initial != nil {
		opts.Initial = uint64(*in.Initial)
	}

	result, err := bucketAgent.Decrement(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.DecrementResponse{
		Cas:           result.Cas,
		Content:       int64(result.Value),
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Append(ctx context.Context, in *kv_v1.AppendRequest) (*kv_v1.AppendResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.AppendOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Value = in.Content

	if in.Cas != nil {
		opts.Cas = *in.Cas
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Append(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, newDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.AppendResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Prepend(ctx context.Context, in *kv_v1.PrependRequest) (*kv_v1.PrependResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.PrependOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Value = in.Content

	if in.Cas != nil {
		opts.Cas = *in.Cas
	}

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Prepend(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, newDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, newDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err).Err()
	}

	return &kv_v1.PrependResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) LookupIn(ctx context.Context, in *kv_v1.LookupInRequest) (*kv_v1.LookupInResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "temporarily not implemented")
}

func (s *KvServer) MutateIn(ctx context.Context, in *kv_v1.MutateInRequest) (*kv_v1.MutateInResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "temporarily not implemented")
}

func NewKvServer(cbClient *gocbcorex.AgentManager) *KvServer {
	return &KvServer{
		cbClient: cbClient,
	}
}
