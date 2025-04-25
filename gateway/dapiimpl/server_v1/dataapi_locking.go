package server_v1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/commonflags"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/stellar-gateway/dataapiv1"
)

func (s *DataApiServer) LockDocument(
	ctx context.Context, in dataapiv1.LockDocumentRequestObject,
) (dataapiv1.LockDocumentResponseObject, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocumentKey)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.GetAndLockOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = key

	if in.Body.LockTime != nil {
		opts.LockTime = *in.Body.LockTime
	}

	result, err := bucketAgent.GetAndLock(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocLocked) {
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

	contentEncoding, respValue, errSt :=
		CompressHandler{}.MaybeCompressContent(result.Value, 0, in.Params.AcceptEncoding)
	if errSt != nil {
		return nil, errSt.Err()
	}

	headers := dataapiv1.LockDocument200ResponseHeaders{
		ETag:            casToHttpEtag(result.Cas),
		XCBFlags:        uint32(result.Flags),
		ContentEncoding: contentEncoding,
	}

	var contentType string
	dataType, _ := commonflags.Decode(result.Flags)
	if result.Flags == 0 {
		// this is special handling for the legacy flags case where the datatype
		// is not set. We need to guess the type based on the content.
		dataType = commonflags.UnknownType
	}
	switch dataType {
	default:
		if json.Valid(result.Value) {
			contentType = "application/json"
		} else if utf8.Valid(result.Value) {
			contentType = "text/plain"
		} else {
			contentType = "application/octet-stream"
		}
	case commonflags.JSONType:
		contentType = "application/json"
	case commonflags.StringType:
		contentType = "text/plain"
	case commonflags.BinaryType:
		contentType = "application/octet-stream"
	}

	return dataapiv1.LockDocument200AsteriskResponse{
		Body:          bytes.NewReader(respValue),
		Headers:       headers,
		ContentType:   contentType,
		ContentLength: int64(len(respValue)),
	}, nil
}

func (s *DataApiServer) UnlockDocument(
	ctx context.Context, in dataapiv1.UnlockDocumentRequestObject,
) (dataapiv1.UnlockDocumentResponseObject, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocumentKey)
	if errSt != nil {
		return nil, errSt.Err()
	}

	cas, errSt := s.parseCAS(in.Params.IfMatch)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.UnlockOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = key
	opts.Cas = cas

	result, err := bucketAgent.Unlock(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else if errors.Is(err, memdx.ErrDocNotLocked) {
			return nil, s.errorHandler.NewDocNotLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
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

	return dataapiv1.UnlockDocument200Response{
		Headers: dataapiv1.UnlockDocument200ResponseHeaders{
			XCBMutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
		},
	}, nil
}
