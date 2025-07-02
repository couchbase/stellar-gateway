package server_v1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/commonflags"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/stellar-gateway/dataapiv1"
)

func (s *DataApiServer) GetDocument(
	ctx context.Context, in dataapiv1.GetDocumentRequestObject,
) (dataapiv1.GetDocumentResponseObject, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocumentKey)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.GetExOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = key
	opts.WithExpiry = true
	opts.WithFlags = true
	if in.Params.Project != nil {
		opts.Project = *in.Params.Project
	}

	result, err := bucketAgent.GetEx(ctx, &opts)
	if err != nil {
		var pathErr *gocbcorex.PathProjectionError
		var path string
		if errors.As(err, &pathErr) {
			path = pathErr.Path
		}

		if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrSubDocNotJSON) {
			return nil, s.errorHandler.NewSdDocNotJsonStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else if errors.Is(err, memdx.ErrSubDocPathInvalid) {
			return nil, s.errorHandler.NewSdPathInvalidStatus(err, path).Err()
		} else if errors.Is(err, memdx.ErrSubDocPathMismatch) {
			return nil, s.errorHandler.NewSdPathMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey, path).Err()
		}

		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	var expiryTime time.Time
	if result.Expiry > 0 {
		expiryTime = time.Unix(int64(result.Expiry), 0)
	}

	contentEncoding, respValue, errSt :=
		CompressHandler{}.MaybeCompressContent(result.Value, 0, in.Params.AcceptEncoding)
	if errSt != nil {
		return nil, errSt.Err()
	}

	headers := dataapiv1.GetDocument200ResponseHeaders{
		ETag:            casToHttpEtag(result.Cas),
		Expires:         timeToHttpTime(expiryTime),
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
	case commonflags.JSONType:
		contentType = "application/json"
	case commonflags.StringType:
		contentType = "text/plain"
	case commonflags.BinaryType:
		contentType = "application/octet-stream"
	default:
		if json.Valid(result.Value) {
			contentType = "application/json"
		} else if utf8.Valid(result.Value) {
			contentType = "text/plain"
		} else {
			contentType = "application/octet-stream"
		}
	}

	return dataapiv1.GetDocument200AsteriskResponse{
		Body:          bytes.NewReader(respValue),
		Headers:       headers,
		ContentType:   contentType,
		ContentLength: int64(len(respValue)),
	}, nil
}

func (s *DataApiServer) CreateDocument(
	ctx context.Context, in dataapiv1.CreateDocumentRequestObject,
) (dataapiv1.CreateDocumentResponseObject, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocumentKey)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var flags uint32
	switch in.ContentType {
	case "application/json":
		flags = commonflags.Encode(commonflags.JSONType, commonflags.NoCompression)
	case "text/plain":
		flags = commonflags.Encode(commonflags.StringType, commonflags.NoCompression)
	case "application/octet-stream":
		flags = commonflags.Encode(commonflags.BinaryType, commonflags.NoCompression)
	}

	if in.Params.XCBFlags != nil {
		flags = *in.Params.XCBFlags
	}

	docValue, err := readDocFromHttpBody(in.Body)
	if err != nil {
		if errors.Is(err, ErrSizeLimitExceeded) {
			return nil, s.errorHandler.NewContentTooLargeStatus().Err()
		} else {
			return nil, s.errorHandler.NewGenericStatus(err).Err()
		}
	}

	var opts gocbcorex.AddOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = key
	opts.Flags = flags

	if in.Params.ContentEncoding != nil {
		switch *in.Params.ContentEncoding {
		case dataapiv1.DocumentEncodingSnappy:
			opts.Value = docValue
			opts.Datatype = opts.Datatype | memdx.DatatypeFlagCompressed
		case dataapiv1.DocumentEncodingIdentity:
			opts.Value = docValue
		default:
			return nil, s.errorHandler.NewInvalidContentEncodingStatus(*in.Params.ContentEncoding).Err()
		}
	} else {
		opts.Value = docValue
	}

	if in.Params.Expires != nil {
		expiry, errSt := parseStringToGocbcorexExpiry(*in.Params.Expires)
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

	result, err := bucketAgent.Add(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocExists) {
			return nil, s.errorHandler.NewDocExistsStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrValueTooLarge) {
			return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey, false).Err()
		} else if errors.Is(err, memdx.ErrDurabilityImpossible) {
			return nil, s.errorHandler.NewDurabilityImpossibleStatus(err, in.BucketName).Err()
		} else if errors.Is(err, memdx.ErrSyncWriteAmbiguous) {
			return nil, s.errorHandler.NewSyncWriteAmbiguousStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else if errors.Is(err, memdx.ErrInvalidArgument) {
			errType := memdx.ParseInvalidArgsError(err)
			if errType == memdx.InvalidArgsErrorCannotInflate {
				return nil, s.errorHandler.NewInvalidSnappyValueError().Err()
			}
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return dataapiv1.CreateDocument200Response{
		Headers: dataapiv1.CreateDocument200ResponseHeaders{
			ETag:             casToHttpEtag(result.Cas),
			XCBMutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
		},
	}, nil
}

func (s *DataApiServer) UpdateDocument(
	ctx context.Context, in dataapiv1.UpdateDocumentRequestObject,
) (dataapiv1.UpdateDocumentResponseObject, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocumentKey)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var isReplace bool
	var cas uint64
	if in.Params.IfMatch != nil {
		if *in.Params.IfMatch == "*" {
			isReplace = true
			cas = 0
		} else {
			parsedCas, errSt := s.parseCAS(in.Params.IfMatch)
			if errSt != nil {
				return nil, errSt.Err()
			}

			isReplace = true
			cas = parsedCas
		}
	} else {
		isReplace = false
		cas = 0
	}

	var flags uint32
	switch in.ContentType {
	case "application/json":
		flags = commonflags.Encode(commonflags.JSONType, commonflags.NoCompression)
	case "text/plain":
		flags = commonflags.Encode(commonflags.StringType, commonflags.NoCompression)
	case "application/octet-stream":
		flags = commonflags.Encode(commonflags.BinaryType, commonflags.NoCompression)
	}

	if in.Params.XCBFlags != nil {
		flags = *in.Params.XCBFlags
	}

	docValue, err := readDocFromHttpBody(in.Body)
	if err != nil {
		if errors.Is(err, ErrSizeLimitExceeded) {
			return nil, s.errorHandler.NewContentTooLargeStatus().Err()
		} else {
			return nil, s.errorHandler.NewGenericStatus(err).Err()
		}
	}

	var datatype memdx.DatatypeFlag
	if in.Params.ContentEncoding != nil {
		switch *in.Params.ContentEncoding {
		case dataapiv1.DocumentEncodingSnappy:
			datatype = datatype | memdx.DatatypeFlagCompressed
		case dataapiv1.DocumentEncodingIdentity:
			// no compression
		default:
			return nil, s.errorHandler.NewInvalidContentEncodingStatus(*in.Params.ContentEncoding).Err()
		}
	}

	var preserveExpiry bool
	var expiry uint32
	if in.Params.Expires != nil {
		parsedExpiry, errSt := parseStringToGocbcorexExpiry(*in.Params.Expires)
		if errSt != nil {
			return nil, errSt.Err()
		}

		preserveExpiry = false
		expiry = parsedExpiry
	} else {
		preserveExpiry = true
		expiry = 0
	}

	var durabilityLevel memdx.DurabilityLevel
	if in.Params.XCBDurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.Params.XCBDurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		durabilityLevel = dl
	}

	if !isReplace {
		var opts gocbcorex.UpsertOptions
		opts.OnBehalfOf = oboUser
		opts.ScopeName = in.ScopeName
		opts.CollectionName = in.CollectionName
		opts.Key = key
		opts.Flags = flags
		opts.Cas = cas
		opts.Value = docValue
		opts.Datatype = datatype
		opts.Expiry = expiry
		opts.PreserveExpiry = preserveExpiry
		opts.DurabilityLevel = durabilityLevel

		result, err := bucketAgent.Upsert(ctx, &opts)
		if err != nil {
			if errors.Is(err, memdx.ErrDocLocked) {
				return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
			} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
				return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
			} else if errors.Is(err, memdx.ErrUnknownScopeName) {
				return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
			} else if errors.Is(err, memdx.ErrAccessError) {
				return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
			} else if errors.Is(err, memdx.ErrValueTooLarge) {
				return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey, false).Err()
			} else if errors.Is(err, memdx.ErrDurabilityImpossible) {
				return nil, s.errorHandler.NewDurabilityImpossibleStatus(err, in.BucketName).Err()
			} else if errors.Is(err, memdx.ErrSyncWriteAmbiguous) {
				return nil, s.errorHandler.NewSyncWriteAmbiguousStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
			} else if errors.Is(err, memdx.ErrInvalidArgument) {
				errType := memdx.ParseInvalidArgsError(err)
				if errType == memdx.InvalidArgsErrorCannotInflate {
					return nil, s.errorHandler.NewInvalidSnappyValueError().Err()
				}
			}

			return nil, s.errorHandler.NewGenericStatus(err).Err()
		}

		return dataapiv1.UpdateDocument200Response{
			Headers: dataapiv1.UpdateDocument200ResponseHeaders{
				ETag:             casToHttpEtag(result.Cas),
				XCBMutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
			},
		}, nil
	} else {
		var opts gocbcorex.ReplaceOptions
		opts.OnBehalfOf = oboUser
		opts.ScopeName = in.ScopeName
		opts.CollectionName = in.CollectionName
		opts.Key = key
		opts.Flags = flags
		opts.Cas = cas
		opts.Value = docValue
		opts.Datatype = datatype
		opts.Expiry = expiry
		opts.PreserveExpiry = preserveExpiry
		opts.DurabilityLevel = durabilityLevel

		result, err := bucketAgent.Replace(ctx, &opts)
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
			} else if errors.Is(err, memdx.ErrValueTooLarge) {
				return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey, false).Err()
			} else if errors.Is(err, memdx.ErrInvalidArgument) {
				errType := memdx.ParseInvalidArgsError(err)
				if errType == memdx.InvalidArgsErrorCannotInflate {
					return nil, s.errorHandler.NewInvalidSnappyValueError().Err()
				}
			}

			return nil, s.errorHandler.NewGenericStatus(err).Err()
		}

		return dataapiv1.UpdateDocument200Response{
			Headers: dataapiv1.UpdateDocument200ResponseHeaders{
				ETag:             casToHttpEtag(result.Cas),
				XCBMutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
			},
		}, nil
	}
}

func (s *DataApiServer) DeleteDocument(
	ctx context.Context, in dataapiv1.DeleteDocumentRequestObject,
) (dataapiv1.DeleteDocumentResponseObject, error) {
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

	var opts gocbcorex.DeleteOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = key
	opts.Cas = cas

	if in.Params.XCBDurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.Params.XCBDurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Delete(ctx, &opts)
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
		} else if errors.Is(err, memdx.ErrDurabilityImpossible) {
			return nil, s.errorHandler.NewDurabilityImpossibleStatus(err, in.BucketName).Err()
		} else if errors.Is(err, memdx.ErrSyncWriteAmbiguous) {
			return nil, s.errorHandler.NewSyncWriteAmbiguousStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return dataapiv1.DeleteDocument200Response{
		Headers: dataapiv1.DeleteDocument200ResponseHeaders{
			ETag:             casToHttpEtag(result.Cas),
			XCBMutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
		},
	}, nil
}
