package server_v1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/stellar-gateway/dataapiv1"
)

func (s *DataApiServer) TouchDocument(
	ctx context.Context, in dataapiv1.TouchDocumentRequestObject,
) (dataapiv1.TouchDocumentResponseObject, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocumentKey)
	if errSt != nil {
		return nil, errSt.Err()
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

		return dataapiv1.TouchDocument202Response{
			Headers: dataapiv1.TouchDocument202ResponseHeaders{
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
			CompressHandler{}.MaybeCompressContent(result.Value, 0, in.Params.AcceptEncoding)
		if errSt != nil {
			return nil, errSt.Err()
		}

		headers := dataapiv1.TouchDocument200ResponseHeaders{
			ETag:            casToHttpEtag(result.Cas),
			XCBFlags:        uint32(result.Flags),
			ContentEncoding: contentEncoding,
		}

		contentType := flagsToHttpContentType(result.Flags)
		switch contentType {
		case "application/json":
			body := make(map[string]interface{})
			err = json.Unmarshal(respValue, &body)
			if err != nil {
				return nil, s.errorHandler.NewSdDocNotJsonStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
			}

			resp := dataapiv1.TouchDocument200JSONResponse{
				Body:    body,
				Headers: headers,
			}

			return resp, nil
		case "text/plain":
			resp := dataapiv1.TouchDocument200TextResponse{
				Body:    string(respValue),
				Headers: headers,
			}

			return resp, nil
		default:
			resp := dataapiv1.TouchDocument200ApplicationoctetStreamResponse{
				Body:          bytes.NewReader(respValue),
				Headers:       headers,
				ContentLength: int64(len(respValue)),
			}

			return resp, nil
		}
	}
}

func parseTouchExpiry(when string) (uint32, *Status) {
	expiry, errSt := httpTimeToGocbcorexExpiry(when)
	if errSt == nil {
		return expiry, nil
	}

	expiry, errSt = isoTimeToGocbcorexExpiry(when)
	if errSt == nil {
		return expiry, nil
	}

	return 0, &Status{
		StatusCode: http.StatusBadRequest,
		Message:    "Invalid time format - expected RFC1123 or RFC3339.",
	}
}
