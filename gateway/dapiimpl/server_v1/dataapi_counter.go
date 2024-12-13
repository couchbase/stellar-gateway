package server_v1

import (
	"context"
	"errors"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/stellar-gateway/dataapiv1"
)

func (s *DataApiServer) IncrementDocument(
	ctx context.Context, in dataapiv1.IncrementDocumentRequestObject,
) (dataapiv1.IncrementDocumentResponseObject, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocumentKey)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.IncrementOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = key
	if in.Body.Delta != nil {
		opts.Delta = uint64(*in.Body.Delta)
	}
	if in.Body.Initial != nil {
		opts.Initial = uint64(*in.Body.Initial)
	} else {
		opts.Initial = 0xffffffffffffffff
	}

	if in.Params.Expires != nil {
		expiry, errSt := httpTimeToGocbcorexExpiry(*in.Params.Expires)
		if errSt != nil {
			return nil, errSt.Err()
		}

		opts.Expiry = expiry
	}

	if in.Params.XCBDurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.Params.XCBDurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Increment(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return dataapiv1.IncrementDocument200JSONResponse{
		Headers: dataapiv1.IncrementDocument200ResponseHeaders{
			ETag:             casToHttpEtag(result.Cas),
			XCBMutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
		},
		Body: result.Value,
	}, nil
}

func (s *DataApiServer) DecrementDocument(
	ctx context.Context, in dataapiv1.DecrementDocumentRequestObject,
) (dataapiv1.DecrementDocumentResponseObject, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocumentKey)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.DecrementOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = key
	if in.Body.Delta != nil {
		opts.Delta = uint64(*in.Body.Delta)
	}
	if in.Body.Initial != nil {
		opts.Initial = uint64(*in.Body.Initial)
	} else {
		opts.Initial = 0xffffffffffffffff
	}

	if in.Params.Expires != nil {
		expiry, errSt := httpTimeToGocbcorexExpiry(*in.Params.Expires)
		if errSt != nil {
			return nil, errSt.Err()
		}

		opts.Expiry = expiry
	}

	if in.Params.XCBDurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.Params.XCBDurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Decrement(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return dataapiv1.DecrementDocument200JSONResponse{
		Headers: dataapiv1.DecrementDocument200ResponseHeaders{
			ETag:             casToHttpEtag(result.Cas),
			XCBMutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
		},
		Body: result.Value,
	}, nil
}
