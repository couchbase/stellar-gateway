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
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KvServer struct {
	kv_v1.UnimplementedKvServiceServer

	logger   *zap.Logger
	cbClient *gocbcorex.AgentManager
}

func NewKvServer(
	logger *zap.Logger,
	cbClient *gocbcorex.AgentManager,
) *KvServer {
	return &KvServer{
		logger:   logger,
		cbClient: cbClient,
	}
}

func (s *KvServer) getBucketAgent(
	ctx context.Context, bucketName string,
) (*gocbcorex.Agent, *status.Status) {
	bucketAgent, err := s.cbClient.GetBucketAgent(ctx, bucketName)
	if err != nil {
		return nil, cbGenericErrToPsStatus(err, s.logger)
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
		return bytes, kv_v1.DocumentContentType_DOCUMENT_CONTENT_TYPE_BINARY, nil
	} else if valueType == gocbcore.StringType {
		// we don't support string data types in this case and instead just
		// handle them as raw binary data instead...
		// TODO(brett19): Decide how to handle string transcoding better.
		return bytes, kv_v1.DocumentContentType_DOCUMENT_CONTENT_TYPE_BINARY, nil
	} else if valueType == gocbcore.JSONType {
		return bytes, kv_v1.DocumentContentType_DOCUMENT_CONTENT_TYPE_JSON, nil
	}

	return nil, 0, status.New(
		codes.Internal,
		"An unexpected value type was specified by the document.")
}

func (s *KvServer) encodeContent(
	contentBytes []byte, contentType kv_v1.DocumentContentType,
) ([]byte, uint32, *status.Status) {
	if contentType == kv_v1.DocumentContentType_DOCUMENT_CONTENT_TYPE_BINARY {
		return contentBytes, gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression), nil
	} else if contentType == kv_v1.DocumentContentType_DOCUMENT_CONTENT_TYPE_JSON {
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

	var opts gocbcorex.LookupInOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Ops = []memdx.LookupInOp{
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
	}

	result, err := bucketAgent.LookupIn(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
	}

	expiryTimeSecs, err := strconv.ParseInt(string(result.Ops[0].Value), 10, 64)
	if err != nil {
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
	}

	flags, err := strconv.ParseUint(string(result.Ops[1].Value), 10, 64)
	if err != nil {
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
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

func (s *KvServer) GetAndTouch(ctx context.Context, in *kv_v1.GetAndTouchRequest) (*kv_v1.GetAndTouchResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.GetAndTouchOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)

	switch expirySpec := in.Expiry.(type) {
	case *kv_v1.GetAndTouchRequest_ExpiryTime:
		opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
	case *kv_v1.GetAndTouchRequest_ExpirySecs:
		opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
	default:
		return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
	}

	result, err := bucketAgent.GetAndTouch(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
	}

	contentBytes, contentType, errSt := s.parseContent(result.Value, result.Flags)
	if errSt != nil {
		return nil, errSt.Err()
	}

	return &kv_v1.GetAndTouchResponse{
		Content:     contentBytes,
		ContentType: contentType,
		Cas:         result.Cas,
	}, nil
}

func (s *KvServer) GetAndLock(ctx context.Context, in *kv_v1.GetAndLockRequest) (*kv_v1.GetAndLockResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.GetAndLockOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.LockTime = in.LockTime

	result, err := bucketAgent.GetAndLock(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
	}

	contentBytes, contentType, errSt := s.parseContent(result.Value, result.Flags)
	if errSt != nil {
		return nil, errSt.Err()
	}

	return &kv_v1.GetAndLockResponse{
		Content:     contentBytes,
		ContentType: contentType,
		Cas:         result.Cas,
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
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
	}

	return &kv_v1.InsertResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) Exists(ctx context.Context, in *kv_v1.ExistsRequest) (*kv_v1.ExistsResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.GetMetaOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)

	result, err := bucketAgent.GetMeta(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return &kv_v1.ExistsResponse{
				Result: false,
			}, nil
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
	}

	resp := &kv_v1.ExistsResponse{
		Result: result.Deleted == 0,
	}

	if resp.Result {
		resp.Cas = result.Cas
	}

	return resp, nil
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
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
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
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
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
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
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
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
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
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
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
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
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
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
	}

	return &kv_v1.PrependResponse{
		Cas:           result.Cas,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}

func (s *KvServer) LookupIn(ctx context.Context, in *kv_v1.LookupInRequest) (*kv_v1.LookupInResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.LookupInOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)

	ops := make([]memdx.LookupInOp, len(in.Specs))
	for i, spec := range in.Specs {
		var op memdx.LookupInOpType
		switch spec.Operation {
		case kv_v1.LookupInRequest_Spec_OPERATION_GET:
			op = memdx.LookupInOpTypeGet
		case kv_v1.LookupInRequest_Spec_OPERATION_COUNT:
			op = memdx.LookupInOpTypeGetCount
		case kv_v1.LookupInRequest_Spec_OPERATION_EXISTS:
			op = memdx.LookupInOpTypeExists
		default:
			return nil, status.New(codes.InvalidArgument, "invalid lookup in op type specified").Err()
		}
		var flags memdx.SubdocOpFlag
		if spec.Flags != nil {
			if spec.Flags.GetXattr() {
				flags = memdx.SubdocOpFlagXattrPath
			}
		}
		ops[i] = memdx.LookupInOp{
			Op:    op,
			Flags: flags,
			Path:  []byte(spec.Path),
		}
	}
	opts.Ops = ops

	if in.Flags != nil {
		if in.Flags.GetAccessDeleted() {
			opts.Flags = opts.Flags | memdx.SubdocDocFlagAccessDeleted
		}
	}

	result, err := bucketAgent.LookupIn(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
	}

	resultSpecs := make([]*kv_v1.LookupInResponse_Spec, len(result.Ops))
	for i, op := range result.Ops {
		spec := &kv_v1.LookupInResponse_Spec{
			Content: op.Value,
		}
		if op.Err != nil {
			if errors.Is(op.Err, memdx.ErrSubDocPathNotFound) {
				st := newSdPathNotFoundStatus(op.Err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[i].Path)
				spec.Status = st.Proto()
			} else if errors.Is(op.Err, memdx.ErrSubDocPathInvalid) {
				st := newSdPathEinvalStatus(op.Err, in.Specs[i].Path)
				spec.Status = st.Proto()
			} else if errors.Is(op.Err, memdx.ErrSubDocPathMismatch) {
				st := newSdPathMismatchStatus(op.Err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[i].Path)
				spec.Status = st.Proto()
			} else {
				spec.Status = cbGenericErrToPsStatus(op.Err, s.logger).Proto()
			}
		}

		resultSpecs[i] = spec
	}

	return &kv_v1.LookupInResponse{
		Cas:   result.Cas,
		Specs: resultSpecs,
	}, nil
}

func (s *KvServer) MutateIn(ctx context.Context, in *kv_v1.MutateInRequest) (*kv_v1.MutateInResponse, error) {
	bucketAgent, errSt := s.getBucketAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.MutateInOptions
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)

	ops := make([]memdx.MutateInOp, len(in.Specs))
	for i, spec := range in.Specs {
		var op memdx.MutateInOpType
		switch spec.Operation {
		case kv_v1.MutateInRequest_Spec_OPERATION_UPSERT:
			op = memdx.MutateInOpTypeDictSet
		case kv_v1.MutateInRequest_Spec_OPERATION_REPLACE:
			op = memdx.MutateInOpTypeReplace
		case kv_v1.MutateInRequest_Spec_OPERATION_REMOVE:
			op = memdx.MutateInOpTypeDelete
		case kv_v1.MutateInRequest_Spec_OPERATION_INSERT:
			op = memdx.MutateInOpTypeDictAdd
		case kv_v1.MutateInRequest_Spec_OPERATION_COUNTER:
			op = memdx.MutateInOpTypeCounter
		case kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_APPEND:
			op = memdx.MutateInOpTypeArrayPushLast
		case kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_ADD_UNIQUE:
			op = memdx.MutateInOpTypeArrayAddUnique
		case kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_INSERT:
			op = memdx.MutateInOpTypeArrayInsert
		case kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_PREPEND:
			op = memdx.MutateInOpTypeArrayPushFirst
		default:
			return nil, status.New(codes.InvalidArgument, "invalid mutate in op type specified").Err()
		}
		var flags memdx.SubdocOpFlag
		if spec.Flags != nil {
			if spec.Flags.GetXattr() {
				flags = memdx.SubdocOpFlagXattrPath
			}
			if spec.Flags.GetCreatePath() {
				flags = memdx.SubdocOpFlagMkDirP
			}
		}
		ops[i] = memdx.MutateInOp{
			Op:    op,
			Flags: flags,
			Path:  []byte(spec.Path),
			Value: spec.Content,
		}
	}
	opts.Ops = ops

	if in.Flags != nil {
		if in.Flags.GetAccessDeleted() {
			opts.Flags = opts.Flags | memdx.SubdocDocFlagAccessDeleted
		}
	}

	if in.Cas != nil {
		opts.Cas = *in.Cas
	}

	// if in.Expiry == nil {
	// 	opts.PreserveExpiry = true
	// } else if in.Expiry != nil {
	// 	if expirySpec, ok := in.Expiry.(*kv_v1.ReplaceRequest_ExpiryTime); ok {
	// 		opts.Expiry = timeExpiryToGocbcorex(timeToGo(expirySpec.ExpiryTime))
	// 	} else if expirySpec, ok := in.Expiry.(*kv_v1.ReplaceRequest_ExpirySecs); ok {
	// 		opts.Expiry = secsExpiryToGocbcorex(expirySpec.ExpirySecs)
	// 	} else {
	// 		return nil, status.New(codes.InvalidArgument, "Expiry time specification is unknown.").Err()
	// 	}
	// }

	if in.DurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.DurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	if in.StoreSemantic != nil {
		switch *in.StoreSemantic {
		case kv_v1.MutateInRequest_STORE_SEMANTIC_REPLACE:
			// This is just the default behaviour
		case kv_v1.MutateInRequest_STORE_SEMANTIC_UPSERT:
			opts.Flags = opts.Flags | memdx.SubdocDocFlagMkDoc
		case kv_v1.MutateInRequest_STORE_SEMANTIC_INSERT:
			opts.Flags = opts.Flags | memdx.SubdocDocFlagAddDoc
		}
	}

	result, err := bucketAgent.MutateIn(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, newDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, newCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else {
			var subdocErr *memdx.SubDocError
			if errors.As(err, &subdocErr) {
				if subdocErr.OpIndex > len(in.Specs) {
					return nil, status.New(codes.Internal, "server responded with error opIndex outside of range of provided specs").Err()
				}

				if errors.Is(err, memdx.ErrSubDocPathNotFound) {
					return nil, newSdPathNotFoundStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[subdocErr.OpIndex].Path).Err()
				} else if errors.Is(err, memdx.ErrSubDocPathInvalid) {
					return nil, newSdPathEinvalStatus(err, in.Specs[subdocErr.OpIndex].Path).Err()
				} else if errors.Is(err, memdx.ErrSubDocPathMismatch) {
					return nil, newSdPathMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, in.Specs[subdocErr.OpIndex].Path).Err()
				}
			}
		}

		return nil, cbGenericErrToPsStatus(err, s.logger).Err()
	}

	resultSpecs := make([]*kv_v1.MutateInResponse_Spec, len(result.Ops))
	for i, op := range result.Ops {
		spec := &kv_v1.MutateInResponse_Spec{
			Content: op.Value,
		}

		resultSpecs[i] = spec
	}

	return &kv_v1.MutateInResponse{
		Cas:           result.Cas,
		Specs:         resultSpecs,
		MutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
	}, nil
}
