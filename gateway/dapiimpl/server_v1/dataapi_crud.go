package server_v1

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/commonflags"
	"github.com/couchbase/gocbcorex/helpers/subdocpath"
	"github.com/couchbase/gocbcorex/helpers/subdocprojection"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/stellar-gateway/dataapiv1"
	"go.uber.org/zap"
)

func (s *DataApiServer) GetDocument(
	ctx context.Context, in dataapiv1.GetDocumentRequestObject,
) (dataapiv1.GetDocumentResponseObject, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocKey)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var executeGet func(forceFullDoc bool) (dataapiv1.GetDocumentResponseObject, error)
	executeGet = func(forceFullDoc bool) (dataapiv1.GetDocumentResponseObject, error) {
		var opts gocbcorex.LookupInOptions
		opts.OnBehalfOf = oboUser
		opts.ScopeName = in.ScopeName
		opts.CollectionName = in.CollectionName
		opts.Key = key

		opts.Ops = append(opts.Ops, memdx.LookupInOp{
			Op:    memdx.LookupInOpTypeGet,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte("$document.exptime"),
		})
		opts.Ops = append(opts.Ops, memdx.LookupInOp{
			Op:    memdx.LookupInOpTypeGet,
			Flags: memdx.SubdocOpFlagXattrPath,
			Path:  []byte("$document.flags"),
		})

		var projectKeys []string
		if in.Params.Project != nil {
			projectKeys = *in.Params.Project
		}

		userProjectOffset := len(opts.Ops)
		maxUserProjections := 16 - userProjectOffset

		isFullDocFetch := false
		if len(projectKeys) > 0 && len(projectKeys) < maxUserProjections && !forceFullDoc {
			for _, projectPath := range projectKeys {
				opts.Ops = append(opts.Ops, memdx.LookupInOp{
					Op:    memdx.LookupInOpTypeGet,
					Flags: memdx.SubdocOpFlagNone,
					Path:  []byte(projectPath),
				})
			}

			isFullDocFetch = false
		} else {
			opts.Ops = append(opts.Ops, memdx.LookupInOp{
				Op:    memdx.LookupInOpTypeGetDoc,
				Flags: memdx.SubdocOpFlagNone,
				Path:  nil,
			})

			isFullDocFetch = true
		}

		result, err := bucketAgent.LookupIn(ctx, &opts)
		if err != nil {
			if errors.Is(err, memdx.ErrDocLocked) {
				return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocKey).Err()
			} else if errors.Is(err, memdx.ErrDocNotFound) {
				return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocKey).Err()
			} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
				return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
			} else if errors.Is(err, memdx.ErrUnknownScopeName) {
				return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
			} else if errors.Is(err, memdx.ErrAccessError) {
				return nil, s.errorHandler.NewCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
			}
			return nil, s.errorHandler.NewGenericStatus(err).Err()
		}

		expiryTimeSecs, err := strconv.ParseInt(string(result.Ops[0].Value), 10, 64)
		if err != nil {
			return nil, s.errorHandler.NewGenericStatus(err).Err()
		}

		var expiryTime time.Time
		if expiryTimeSecs > 0 {
			expiryTime = time.Unix(expiryTimeSecs, 0)
		}

		flags64, err := strconv.ParseUint(string(result.Ops[1].Value), 10, 64)
		if err != nil {
			return nil, s.errorHandler.NewGenericStatus(err).Err()
		}
		flags := uint32(flags64)

		if len(projectKeys) > 0 {
			var writer subdocprojection.Projector

			if isFullDocFetch {
				docValue := result.Ops[2].Value

				var reader subdocprojection.Projector

				err := reader.Init(docValue)
				if err != nil {
					return nil, s.errorHandler.NewGenericStatus(err).Err()
				}

				for _, path := range projectKeys {
					parsedPath, err := subdocpath.Parse(path)
					if err != nil {
						return nil, s.errorHandler.NewGenericStatus(err).Err()
					}

					pathValue, err := reader.Get(parsedPath)
					if err != nil {
						return nil, s.errorHandler.NewGenericStatus(err).Err()
					}

					err = writer.Set(parsedPath, pathValue)
					if err != nil {
						return nil, s.errorHandler.NewGenericStatus(err).Err()
					}
				}
			} else {
				for pathIdx, path := range projectKeys {
					op := result.Ops[userProjectOffset+pathIdx]

					if op.Err != nil {
						if errors.Is(op.Err, memdx.ErrSubDocDocTooDeep) {
							s.logger.Debug("falling back to fulldoc projection due to ErrSubDocDocTooDeep")
							return executeGet(true)
						} else if errors.Is(op.Err, memdx.ErrSubDocNotJSON) {
							return nil, s.errorHandler.NewSdDocNotJsonStatus(op.Err, in.BucketName, in.ScopeName, in.CollectionName, in.DocKey).Err()
						} else if errors.Is(op.Err, memdx.ErrSubDocPathNotFound) {
							// path not founds are skipped and not included in the
							// output document rather than triggering errors.
							continue
						} else if errors.Is(op.Err, memdx.ErrSubDocPathInvalid) {
							return nil, s.errorHandler.NewSdPathInvalidStatus(op.Err, projectKeys[pathIdx]).Err()
						} else if errors.Is(op.Err, memdx.ErrSubDocPathMismatch) {
							return nil, s.errorHandler.NewSdPathMismatchStatus(op.Err, in.BucketName, in.ScopeName, in.CollectionName, in.DocKey, projectKeys[pathIdx]).Err()
						} else if errors.Is(op.Err, memdx.ErrSubDocPathTooBig) {
							s.logger.Debug("falling back to fulldoc projection due to ErrSubDocPathTooBig")
							return executeGet(true)
						}

						s.logger.Debug("falling back to fulldoc projection due to unexpected op error", zap.Error(op.Err))
						return executeGet(true)
					}

					parsedPath, err := subdocpath.Parse(path)
					if err != nil {
						return nil, s.errorHandler.NewGenericStatus(err).Err()
					}

					err = writer.Set(parsedPath, op.Value)
					if err != nil {
						return nil, s.errorHandler.NewGenericStatus(err).Err()
					}
				}
			}

			projectedDocValue, err := writer.Build()
			if errSt != nil {
				return nil, s.errorHandler.NewGenericStatus(err).Err()
			}

			return dataapiv1.GetDocument200AsteriskResponse{
				Body:          bytes.NewReader(projectedDocValue),
				ContentLength: int64(len(projectedDocValue)),
				ContentType:   "application/json",
				Headers: dataapiv1.GetDocument200ResponseHeaders{
					ETag:     casToHttpEtag(result.Cas),
					Expires:  timeToHttpTime(expiryTime),
					XCBFlags: uint32(0),
				},
			}, nil
		}

		docValue := result.Ops[2].Value

		resp := dataapiv1.GetDocument200AsteriskResponse{
			Headers: dataapiv1.GetDocument200ResponseHeaders{
				ETag:     casToHttpEtag(result.Cas),
				Expires:  timeToHttpTime(expiryTime),
				XCBFlags: uint32(flags),
			},
		}

		contentType := flagsToHttpContentType(flags)

		contentEncoding, respValue, errSt :=
			CompressHandler{}.MaybeCompressContent(docValue, 0, in.Params.AcceptEncoding)
		if errSt != nil {
			return nil, errSt.Err()
		}

		resp.ContentType = contentType
		resp.Headers.ContentEncoding = contentEncoding
		resp.Body = bytes.NewReader(respValue)
		resp.ContentLength = int64(len(respValue))

		return resp, nil
	}

	return executeGet(false)
}

func (s *DataApiServer) CreateDocument(
	ctx context.Context, in dataapiv1.CreateDocumentRequestObject,
) (dataapiv1.CreateDocumentResponseObject, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocKey)
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

	docValue, err := io.ReadAll(io.LimitReader(in.Body, 20*1024*1024))
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
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

	result, err := bucketAgent.Add(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocExists) {
			return nil, s.errorHandler.NewDocExistsStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocKey).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrValueTooLarge) {
			return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocKey, false).Err()
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

	key, errSt := s.parseKey(in.DocKey)
	if errSt != nil {
		return nil, errSt.Err()
	}

	cas, errSt := s.parseCAS(in.Params.IfMatch)
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

	docValue, err := io.ReadAll(io.LimitReader(in.Body, 20*1024*1024))
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	var opts gocbcorex.UpsertOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = key
	opts.Flags = flags
	opts.Cas = cas

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
		expiry, errSt := httpTimeToGocbcorexExpiry(*in.Params.Expires)
		if errSt != nil {
			return nil, errSt.Err()
		}

		opts.PreserveExpiry = false
		opts.Expiry = expiry
	} else {
		opts.PreserveExpiry = true
		opts.Expiry = 0
	}

	if in.Params.XCBDurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.Params.XCBDurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.Upsert(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocKey).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrValueTooLarge) {
			return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocKey, false).Err()
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

func (s *DataApiServer) DeleteDocument(
	ctx context.Context, in dataapiv1.DeleteDocumentRequestObject,
) (dataapiv1.DeleteDocumentResponseObject, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocKey)
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
			return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocKey).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocKey).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocKey).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
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
