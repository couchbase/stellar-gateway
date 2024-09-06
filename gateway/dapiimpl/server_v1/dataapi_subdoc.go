package server_v1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/stellar-gateway/dataapiv1"
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
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	resp := dataapiv1.LookupInDocument200JSONResponse{
		Headers: dataapiv1.LookupInDocument200ResponseHeaders{
			ETag: casToHttpEtag(result.Cas),
		},
	}

	for _, op := range result.Ops {
		var errOut *string
		var valueOut *interface{}

		if op.Err != nil {
			errText := op.Err.Error()
			errOut = &errText
		} else {
			if op.Value != nil {
				var jsonValue interface{} = json.RawMessage(op.Value)
				valueOut = &jsonValue
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

		opts.Ops = append(opts.Ops, memdx.MutateInOp{
			Op:   opType,
			Path: path,
		})
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
			return nil, s.errorHandler.NewCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrValueTooLarge) {
			if in.Body.StoreSemantic != nil && *in.Body.StoreSemantic == dataapiv1.StoreSemanticInsert {
				return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey, false).Err()
			}
			// We have no way to differentiate whether the document already existed here, so just treat it as expanding.
			return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey, true).Err()
		} else if errors.Is(err, memdx.ErrSubDocInvalidCombo) {
			// TODO(brett19): We should handle these errors like we do in Protostellar
		} else if errors.Is(err, memdx.ErrDocExists) {
			return nil, s.errorHandler.NewDocExistsStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.DocumentKey).Err()
		} else {
			var subdocErr *memdx.SubDocError
			if errors.As(err, &subdocErr) {
				if subdocErr.OpIndex >= len(*in.Body.Operations) {
					return nil, Status{
						StatusCode: http.StatusInternalServerError,
						Message:    "server responded with error opIndex outside of range of provided specs",
					}.Err()
				}

				// TODO(brett19): We should handle these errors like we do in Protostellar
			}
		}

		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	resp := dataapiv1.MutateInDocument200JSONResponse{
		Headers: dataapiv1.MutateInDocument200ResponseHeaders{
			ETag: casToHttpEtag(result.Cas),
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
