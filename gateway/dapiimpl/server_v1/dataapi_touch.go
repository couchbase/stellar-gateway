package server_v1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"unicode/utf8"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/commonflags"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/stellar-gateway/dataapiv1"
)

func (s *DataApiServer) TouchDocument(
	ctx context.Context, in dataapiv1.TouchDocumentRequestObject,
) (dataapiv1.TouchDocumentResponseObject, error) {
	ctx, cancel := s.withKvTimeout(ctx)
	defer cancel()

	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocumentKey)
	if errSt != nil {
		return nil, errSt.Err()
	}

	if in.Body.Expiry == nil {
		return nil, s.errorHandler.NewTouchMissingExpiryStatus().Err()
	}

	var newExpiry uint32
	if in.Body.Expiry != nil {
		expiry, errSt := parseTouchExpiry(*in.Body.Expiry)
		if errSt != nil {
			return nil, errSt.Err()
		}

		newExpiry = expiry
	}

	if in.Body.ReturnContent == nil || !*in.Body.ReturnContent {
		var opts gocbcorex.TouchOptions
		opts.OnBehalfOf = oboUser
		opts.ScopeName = in.ScopeName
		opts.CollectionName = in.CollectionName
		opts.Key = key
		opts.Expiry = newExpiry

		result, err := bucketAgent.Touch(ctx, &opts)
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

		return dataapiv1.TouchDocument204Response{
			Headers: dataapiv1.TouchDocument204ResponseHeaders{
				ETag: casToHttpEtag(result.Cas),
			},
		}, nil
	} else {
		var opts gocbcorex.GetAndTouchOptions
		opts.OnBehalfOf = oboUser
		opts.ScopeName = in.ScopeName
		opts.CollectionName = in.CollectionName
		opts.Key = key
		opts.Expiry = newExpiry

		result, err := bucketAgent.GetAndTouch(ctx, &opts)
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
			CompressHandler{}.MaybeCompressContent(result.Value, result.Datatype, in.Params.AcceptEncoding)
		if errSt != nil {
			return nil, errSt.Err()
		}

		headers := dataapiv1.TouchDocument200ResponseHeaders{
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

		return dataapiv1.TouchDocument200AsteriskResponse{
			Body:          bytes.NewReader(respValue),
			Headers:       headers,
			ContentType:   contentType,
			ContentLength: int64(len(respValue)),
		}, nil
	}
}

func parseTouchExpiry(when string) (uint32, *Status) {
	expiry, errSt := parseStringToGocbcorexExpiry(when)
	if errSt == nil {
		return expiry, nil
	}

	expiry, errSt = isoTimeToGocbcorexExpiry(when)
	if errSt == nil {
		return expiry, nil
	}

	return 0, &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    "Invalid time format - expected RFC1123 or RFC3339.",
	}
}
