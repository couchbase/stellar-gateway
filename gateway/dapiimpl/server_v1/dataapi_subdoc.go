package server_v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/stellar-gateway/dataapiv1"
	"go.uber.org/zap"
)

func (s *DataApiServer) LookupInDocument(
	ctx context.Context, in dataapiv1.LookupInDocumentRequestObject,
) (dataapiv1.LookupInDocumentResponseObject, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocumentKey)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.LookupInOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = key

	if in.Body.Operations == nil {
		return nil, s.errorHandler.NewTooFewOperationsError().Err()
	}
	for opIdx, op := range *in.Body.Operations {
		var path []byte
		if op.Path != nil {
			path = []byte(*op.Path)
		}

		opType, isValidOpType := lookupInOperationToMemdx(op.Operation)
		if !isValidOpType {
			return nil, s.errorHandler.NewInvalidOperationTypeError(opIdx).Err()
		}

		opts.Ops = append(opts.Ops, memdx.LookupInOp{
			Op:   opType,
			Path: path,
		})
	}

	result, err := bucketAgent.LookupIn(ctx, &opts)
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
			return nil, s.errorHandler.NewCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrSubDocInvalidCombo) {
			return nil, s.errorHandler.NewSdBadCombo(err).Err()
		} else if errors.Is(err, memdx.ErrInvalidArgument) {
			return nil, s.errorHandler.NewSubdocInvalidArgStatus(err).Err()
		}

		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	for _, op := range result.Ops {
		if op.Err != nil {
			if errors.Is(op.Err, memdx.ErrSubDocDocTooDeep) {
				return nil, s.errorHandler.NewSdDocTooDeepStatus(op.Err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
			} else if errors.Is(op.Err, memdx.ErrSubDocNotJSON) {
				return nil, s.errorHandler.NewSdDocNotJsonStatus(op.Err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
			}
		}
	}

	resp := dataapiv1.LookupInDocument200JSONResponse{
		Headers: dataapiv1.LookupInDocument200ResponseHeaders{
			ETag: casToHttpEtag(result.Cas),
		},
	}

	inSpecs := *in.Body.Operations
	for opIdx, op := range result.Ops {
		var errOut *dataapiv1.SubdocError
		var valueOut interface{}

		if op.Err != nil {
			if errors.Is(op.Err, memdx.ErrSubDocPathNotFound) {
				errOut = &dataapiv1.SubdocError{
					Error:   dataapiv1.SubdocErrorCodePathNotFound,
					Message: "The requested path was not found in the document.",
				}
			} else if errors.Is(op.Err, memdx.ErrSubDocPathInvalid) {
				errOut = &dataapiv1.SubdocError{
					Error:   dataapiv1.SubdocErrorCodeInvalidArgument,
					Message: "Invalid path specified.",
				}
			} else if errors.Is(op.Err, memdx.ErrSubDocPathMismatch) {
				errOut = &dataapiv1.SubdocError{
					Error:   dataapiv1.SubdocErrorCodePathMismatch,
					Message: "The structure implied by the provided path does not match the document.",
				}
			} else if errors.Is(op.Err, memdx.ErrSubDocPathTooBig) {
				errOut = &dataapiv1.SubdocError{
					Error:   dataapiv1.SubdocErrorCodeInvalidArgument,
					Message: "The specified path was too big.",
				}
			} else if errors.Is(op.Err, memdx.ErrSubDocXattrUnknownVAttr) {
				errOut = &dataapiv1.SubdocError{
					Error:   dataapiv1.SubdocErrorCodeUnknownVattr,
					Message: "The requested xattr virtual attribute was not found in the document.",
				}
			} else {
				s.logger.Debug("failed to translate subdoc error", zap.Error(op.Err))

				if !s.errorHandler.Debug {
					errOut = &dataapiv1.SubdocError{
						Error: dataapiv1.SubdocErrorCodeInternal,
					}

				} else {
					errOut = &dataapiv1.SubdocError{
						Error:   dataapiv1.SubdocErrorCodeInternal,
						Message: fmt.Sprintf("An unexpected error occurred: %s", op.Err.Error()),
					}
				}
			}
		} else {
			if op.Value != nil {
				valueOut = json.RawMessage(op.Value)
			}
		}

		if *inSpecs[opIdx].Operation == dataapiv1.LookupInOperationTypeExists {
			if op.Err == nil {
				errOut = nil
				valueOut = json.RawMessage("true")
			} else if errors.Is(op.Err, memdx.ErrSubDocPathNotFound) {
				errOut = nil
				valueOut = json.RawMessage("false")
			}
		}

		resp.Body = append(resp.Body, dataapiv1.LookupInOperationResult{
			Error: errOut,
			Value: valueOut,
		})
	}

	return resp, nil
}

func (s *DataApiServer) MutateInDocument(
	ctx context.Context, in dataapiv1.MutateInDocumentRequestObject,
) (dataapiv1.MutateInDocumentResponseObject, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.Params.Authorization, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	key, errSt := s.parseKey(in.DocumentKey)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.MutateInOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = key

	if in.Body.StoreSemantic != nil {
		switch *in.Body.StoreSemantic {
		case dataapiv1.StoreSemanticInsert:
			opts.Flags |= memdx.SubdocDocFlagAddDoc
		case dataapiv1.StoreSemanticUpsert:
			opts.Flags |= memdx.SubdocDocFlagMkDoc
		case dataapiv1.StoreSemanticReplace:
			// this is the default
		default:
			return nil, s.errorHandler.NewInvalidStoreSemanticError().Err()
		}
	}

	if in.Body.Operations == nil {
		return nil, s.errorHandler.NewTooFewOperationsError().Err()
	}
	for opIdx, op := range *in.Body.Operations {
		var path []byte
		if op.Path != nil {
			path = []byte(*op.Path)
		}

		opType, isValidOpType := mutateInOperationToMemdx(op.Operation)
		if !isValidOpType {
			return nil, s.errorHandler.NewInvalidOperationTypeError(opIdx).Err()
		}

		var opValue []byte
		if op.Value != nil {
			opValue = *op.Value
		}

		opts.Ops = append(opts.Ops, memdx.MutateInOp{
			Op:    opType,
			Path:  path,
			Value: opValue,
		})
	}

	if opts.Flags&memdx.SubdocDocFlagMkDoc != 0 {
		for opIdx, op := range opts.Ops {
			if op.Op == memdx.MutateInOpTypeDelete ||
				op.Op == memdx.MutateInOpTypeReplace ||
				op.Op == memdx.MutateInOpTypeArrayInsert {
				return nil, s.errorHandler.NewSubDocMkDocSubDocOp(opIdx).Err()
			}
		}
	}

	if in.Params.XCBDurabilityLevel != nil {
		dl, errSt := durabilityLevelToMemdx(*in.Params.XCBDurabilityLevel)
		if errSt != nil {
			return nil, errSt.Err()
		}
		opts.DurabilityLevel = dl
	}

	result, err := bucketAgent.MutateIn(ctx, &opts)
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
			if in.Body.StoreSemantic != nil && *in.Body.StoreSemantic == dataapiv1.StoreSemanticInsert {
				return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey, false).Err()
			}
			// We have no way to differentiate whether the document already existed here, so just treat it as expanding.
			return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey, true).Err()
		} else if errors.Is(err, memdx.ErrSubDocInvalidCombo) {
			return nil, s.errorHandler.NewSdBadCombo(err).Err()
		} else if errors.Is(err, memdx.ErrDocExists) {
			return nil, s.errorHandler.NewDocExistsStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else if errors.Is(err, memdx.ErrDurabilityImpossible) {
			return nil, s.errorHandler.NewDurabilityImpossibleStatus(err, in.BucketName).Err()
		} else if errors.Is(err, memdx.ErrInvalidArgument) {
			return nil, s.errorHandler.NewSubdocInvalidArgStatus(err).Err()
		} else {
			var subdocErr *memdx.SubDocError
			if errors.As(err, &subdocErr) {
				if subdocErr.OpIndex >= len(*in.Body.Operations) {
					return nil, Status{
						StatusCode: http.StatusInternalServerError,
						Message:    "server responded with error opIndex outside of range of provided specs",
					}.Err()
				}

				inSpec := (*in.Body.Operations)[subdocErr.OpIndex]
				inPath := ""
				if inSpec.Path != nil {
					inPath = *inSpec.Path
				}

				if errors.Is(err, memdx.ErrSubDocDocTooDeep) {
					return nil, s.errorHandler.NewSdDocTooDeepStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
				} else if errors.Is(err, memdx.ErrSubDocNotJSON) {
					return nil, s.errorHandler.NewSdDocNotJsonStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
				}

				if errors.Is(err, memdx.ErrSubDocPathNotFound) {
					return nil, s.errorHandler.NewSdPathNotFoundStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey, inPath).Err()
				} else if errors.Is(err, memdx.ErrSubDocPathExists) {
					return nil, s.errorHandler.NewSdPathExistsStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey, inPath).Err()
				} else if errors.Is(err, memdx.ErrSubDocPathInvalid) {
					return nil, s.errorHandler.NewSdPathInvalidStatus(err, inPath).Err()
				} else if errors.Is(err, memdx.ErrSubDocPathMismatch) {
					return nil, s.errorHandler.NewSdPathMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey, inPath).Err()
				} else if errors.Is(err, memdx.ErrSubDocPathTooBig) {
					return nil, s.errorHandler.NewSdPathTooBigStatus(err, inPath).Err()
				} else if errors.Is(err, memdx.ErrSubDocXattrUnknownVAttr) {
					return nil, s.errorHandler.NewSdXattrUnknownVattrStatus(err, inPath).Err()
				}
			}
		}

		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	resp := dataapiv1.MutateInDocument200JSONResponse{
		Headers: dataapiv1.MutateInDocument200ResponseHeaders{
			ETag:             casToHttpEtag(result.Cas),
			XCBMutationToken: tokenFromGocbcorex(in.BucketName, result.MutationToken),
		},
	}

	for _, op := range result.Ops {
		var valueOut *interface{}

		if op.Value != nil {
			var jsonValue interface{} = json.RawMessage(op.Value)
			valueOut = &jsonValue
		}

		resp.Body = append(resp.Body, dataapiv1.MutateInOperationResult{
			Value: valueOut,
		})
	}

	return resp, nil
}
