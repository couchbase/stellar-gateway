package server_v1

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/gocbcorex/cbsearchx"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/stellar-gateway/dataapiv1"
	"go.uber.org/zap"
)

type StatusError struct {
	S Status
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s (status: %d, code: %s, resource: %s, requestId: %s, debug: %s)",
		e.S.Message,
		e.S.StatusCode,
		e.S.Code,
		e.S.Resource,
		e.S.RequestID,
		e.S.Debug)
}

type Status struct {
	StatusCode int                 `json:"-"`
	Code       dataapiv1.ErrorCode `json:"code,omitempty"`
	Message    string              `json:"message,omitempty"`
	Resource   string              `json:"resource,omitempty"`
	RequestID  string              `json:"requestId,omitempty"`
	Debug      string              `json:"debug,omitempty"`
}

func (e Status) Err() error {
	return &StatusError{S: e}
}

type ErrorHandler struct {
	Logger *zap.Logger
	Debug  bool
}

func (e ErrorHandler) tryAttachExtraContext(st *Status, baseErr error) *Status {
	if baseErr == nil {
		return st
	}

	var memdSrvErr *memdx.ServerErrorWithContext
	if errors.As(baseErr, &memdSrvErr) {
		parsedCtx := memdSrvErr.ParseContext()
		if parsedCtx.Ref != "" {
			st.RequestID = parsedCtx.Ref
		}
	}

	if e.Debug {
		st.Debug = baseErr.Error()
	}

	return st
}

func (e ErrorHandler) NewInvalidAuthHeaderStatus(baseErr error) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    "Invalid authorization header format.",
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewNoAuthStatus() *Status {
	st := &Status{
		StatusCode: http.StatusUnauthorized,
		Code:       dataapiv1.ErrorCodeUnauthorized,
		Message:    "You must send authentication to use this endpoint.",
	}
	return st
}

func (e ErrorHandler) NewInvalidCredentialsStatus() *Status {
	st := &Status{
		StatusCode: http.StatusForbidden,
		Code:       dataapiv1.ErrorCodeInvalidAuth,
		Message:    "Your username or password is invalid.",
	}
	return st
}

func (e ErrorHandler) NewInternalStatus() *Status {
	st := &Status{
		StatusCode: http.StatusInternalServerError,
		Code:       dataapiv1.ErrorCodeInternal,
		Message:    "An internal error occurred.",
	}
	return st
}

func (e ErrorHandler) NewUnavailableStatus(err error) *Status {
	st := &Status{
		StatusCode: http.StatusServiceUnavailable,
		Code:       dataapiv1.ErrorCodeUnderlyingServiceUnavailable,
		Message:    "One of the underlying services were not available.",
	}
	st = e.tryAttachExtraContext(st, err)
	return st
}

func (e ErrorHandler) NewUnknownStatus(baseErr error) *Status {
	var memdErr *memdx.ServerError
	if errors.As(baseErr, &memdErr) {
		var errText string

		var memdSrvErr *memdx.ServerErrorWithContext
		if errors.As(baseErr, &memdSrvErr) {
			parsedCtx := memdSrvErr.ParseContext()

			if parsedCtx.Ref != "" && parsedCtx.Text != "" {
				errText = fmt.Sprintf("An unknown memcached error occurred (status: %d, ref: %s, text: %s).",
					memdErr.Status, parsedCtx.Ref, parsedCtx.Text)
			} else if parsedCtx.Ref != "" {
				errText = fmt.Sprintf("An unknown memcached error occurred (status: %d, ref: %s).",
					memdErr.Status, parsedCtx.Ref)
			} else if parsedCtx.Text != "" {
				errText = fmt.Sprintf("An unknown memcached error occurred (status: %d, text: %s).",
					memdErr.Status, parsedCtx.Text)
			}
		} else {
			errText = fmt.Sprintf("An unknown memcached error occurred (status: %d).", memdErr.Status)
		}

		st := &Status{
			StatusCode: http.StatusInternalServerError,
			Code:       dataapiv1.ErrorCodeInternal,
			Message:    errText,
		}
		st = e.tryAttachExtraContext(st, baseErr)
		return st
	}

	var queryErr *cbqueryx.ServerErrors
	if errors.As(baseErr, &queryErr) {
		var queryErrDescs []string
		for _, querySubErr := range queryErr.Errors {
			queryErrDescs = append(queryErrDescs, fmt.Sprintf("%d - %s", querySubErr.Code, querySubErr.Msg))
		}

		st := &Status{
			StatusCode: http.StatusInternalServerError,
			Code:       dataapiv1.ErrorCodeInternal,
			Message:    fmt.Sprintf("An unknown query error occurred (descs: %s).", strings.Join(queryErrDescs, "; ")),
		}
		st = e.tryAttachExtraContext(st, baseErr)
		return st
	}

	var searchErr *cbsearchx.ServerError
	if errors.As(baseErr, &searchErr) {
		st := &Status{
			StatusCode: http.StatusInternalServerError,
			Code:       dataapiv1.ErrorCodeInternal,
			Message:    fmt.Sprintf("An unknown search error occurred (status: %d).", searchErr.StatusCode),
		}
		st = e.tryAttachExtraContext(st, baseErr)
		return st
	}

	var serverErr *cbmgmtx.ServerError
	if errors.As(baseErr, &serverErr) {
		st := &Status{
			StatusCode: http.StatusInternalServerError,
			Code:       dataapiv1.ErrorCodeInternal,
			Message:    fmt.Sprintf("An unknown server error occurred (status: %d).", serverErr.StatusCode),
		}
		st = e.tryAttachExtraContext(st, baseErr)
		return st
	}

	st := &Status{
		StatusCode: http.StatusInternalServerError,
		Code:       dataapiv1.ErrorCodeInternal,
		Message:    "An unknown error occurred.",
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewGenericStatus(err error) *Status {
	// we do not attach context in these cases, since they almost always don't actually
	// make it back to the client to be processed anyways.
	if errors.Is(err, context.Canceled) {
		e.Logger.Debug("handling canceled operation error", zap.Error(err))

		return &Status{
			StatusCode: 499,
			Code:       dataapiv1.ErrorCodeRequestCanceled,
			Message:    "The request was cancelled.",
		}
	} else if errors.Is(err, context.DeadlineExceeded) {
		e.Logger.Debug("handling deadline exceeded operation error", zap.Error(err))

		return &Status{
			StatusCode: http.StatusGatewayTimeout,
			Code:       dataapiv1.ErrorCodeRequestCanceled,
			Message:    "The request deadline was exceeded.",
		}
	}

	// if this is a network dial error, we make the assumption that one of the underlying
	// services neccessary for servicing this request is unavailable.
	var netOpError *net.OpError
	if errors.As(err, &netOpError) && netOpError.Op == "dial" {
		e.Logger.Debug("handling network dial error", zap.Error(err))
		return e.NewUnavailableStatus(err)
	}

	// if we still don't know what kind of error this is, we return 'UNKNOWN'
	e.Logger.Debug("handling unknown error", zap.Error(err))
	return e.NewUnknownStatus(err)
}

func (e ErrorHandler) NewBucketMissingStatus(baseErr error, bucketName string) *Status {
	st := &Status{
		StatusCode: http.StatusNotFound,
		Code:       dataapiv1.ErrorCodeBucketNotFound,
		Message: fmt.Sprintf("Bucket '%s' was not found.",
			bucketName),
		Resource: fmt.Sprintf("/buckets/%s", bucketName),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewContentTooLargeStatus() *Status {
	st := &Status{
		StatusCode: http.StatusRequestEntityTooLarge,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    "The content of the request was too large.",
	}
	return st
}

func (e ErrorHandler) NewInvalidKeyLengthStatus(key string) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    fmt.Sprintf("Length of document key '%s' must be between 1 and 251 characters.", key),
	}
	return st
}

func (e ErrorHandler) NewDocLockedStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *Status {
	st := &Status{
		StatusCode: http.StatusConflict,
		Code:       dataapiv1.ErrorCodeDocumentLocked,
		Message: fmt.Sprintf("Cannot perform a write operation against locked document '%s' in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
			bucketName, scopeName, collectionName, docId),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewDocNotLockedStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeDocumentNotLocked,
		Message: fmt.Sprintf("Cannot unlock an unlocked document '%s' in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
			bucketName, scopeName, collectionName, docId),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewDocExistsStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *Status {
	st := &Status{
		StatusCode: http.StatusConflict,
		Code:       dataapiv1.ErrorCodeDocumentExists,
		Message: fmt.Sprintf("Document '%s' already existed in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
			bucketName, scopeName, collectionName, docId),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewDocMissingStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *Status {
	st := &Status{
		StatusCode: http.StatusNotFound,
		Code:       dataapiv1.ErrorCodeDocumentNotFound,
		Message: fmt.Sprintf("Document '%s' not found in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
			bucketName, scopeName, collectionName, docId),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewCollectionMissingStatus(baseErr error, bucketName, scopeName, collectionName string) *Status {
	st := &Status{
		StatusCode: http.StatusNotFound,
		Code:       dataapiv1.ErrorCodeCollectionNotFound,
		Message: fmt.Sprintf("Collection '%s' not found in '%s/%s'.",
			collectionName, bucketName, scopeName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s",
			bucketName, scopeName, collectionName),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewScopeMissingStatus(baseErr error, bucketName, scopeName string) *Status {
	st := &Status{
		StatusCode: http.StatusNotFound,
		Code:       dataapiv1.ErrorCodeScopeNotFound,
		Message: fmt.Sprintf("Scope '%s' not found in '%s'.",
			scopeName, bucketName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s",
			bucketName, scopeName),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewCollectionNoReadAccessStatus(baseErr error, bucketName, scopeName, collectionName string) *Status {
	st := &Status{
		StatusCode: http.StatusForbidden,
		Code:       dataapiv1.ErrorCodeNoReadAccess,
		Message: fmt.Sprintf("No permissions to read documents from '%s/%s/%s'.",
			bucketName, scopeName, collectionName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s",
			bucketName, scopeName, collectionName),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewCollectionNoWriteAccessStatus(baseErr error, bucketName, scopeName, collectionName string) *Status {
	st := &Status{
		StatusCode: http.StatusForbidden,
		Code:       dataapiv1.ErrorCodeNoWriteAccess,
		Message: fmt.Sprintf("No permissions to write documents into '%s/%s/%s'.",
			bucketName, scopeName, collectionName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s",
			bucketName, scopeName, collectionName),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewDocNotNumericStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeDocumentNotNumeric,
		Message: fmt.Sprintf("Cannot perform counter operation on non-numeric document '%s' in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
			bucketName, scopeName, collectionName, docId),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewValueTooLargeStatus(baseErr error, bucketName, scopeName, collectionName, docId string,
	isExpandingValue bool) *Status {
	var st *Status
	if isExpandingValue {
		st = &Status{
			StatusCode: http.StatusBadRequest,
			Code:       dataapiv1.ErrorCodeValueTooLarge,
			Message: fmt.Sprintf("Updated value '%s' made value too large in '%s/%s/%s'.",
				docId, bucketName, scopeName, collectionName),
			Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
				bucketName, scopeName, collectionName, docId),
		}
	} else {
		st = &Status{
			StatusCode: http.StatusBadRequest,
			Code:       dataapiv1.ErrorCodeInvalidArgument,
			Message: fmt.Sprintf("Value '%s' for new document was too large in '%s/%s/%s'.",
				bucketName, scopeName, collectionName, docId),
		}
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewDurabilityImpossibleStatus(baseErr error, bucketName string) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeDurabilityImpossible,
		Message: fmt.Sprintf("Not enough servers to use this durability level on '%s' bucket.",
			bucketName),
		Resource: fmt.Sprintf("/buckets/%s",
			bucketName),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSyncWriteAmbiguousStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *Status {
	st := &Status{
		StatusCode: http.StatusRequestTimeout,
		Message: fmt.Sprintf("Sync write operation on '%s' in '%s/%s/%s' timed out.",
			docId, bucketName, scopeName, collectionName),
	}
	return st
}

func (e ErrorHandler) NewSdDocTooDeepStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeDocumentTooDeep,
		Message: fmt.Sprintf("Document '%s' JSON structure is too deep in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
			bucketName, scopeName, collectionName, docId),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdDocNotJsonStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeDocumentNotJson,
		Message: fmt.Sprintf("Document '%s' was not JSON in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
			bucketName, scopeName, collectionName, docId),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdPathInvalidStatus(baseErr error, sdPath string) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    fmt.Sprintf("Invalid subdocument path syntax '%s'.", sdPath),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdPathMismatchStatus(baseErr error, bucketName, scopeName, collectionName, docId, sdPath string) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodePathMismatch,
		Message: fmt.Sprintf("Document structure implied by path '%s' did not match document '%s' in '%s/%s/%s'.",
			sdPath, docId, bucketName, scopeName, collectionName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s/content/{%s}",
			bucketName, scopeName, collectionName, docId, sdPath),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdPathNotFoundStatus(baseErr error, bucketName, scopeName, collectionName, docId, sdPath string) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodePathNotFound,
		Message: fmt.Sprintf("Subdocument path '%s' was not found in '%s' in '%s/%s/%s'.",
			sdPath, docId, bucketName, scopeName, collectionName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s/content/{%s}",
			bucketName, scopeName, collectionName, docId, sdPath),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdPathExistsStatus(baseErr error, bucketName, scopeName, collectionName, docId, sdPath string) *Status {
	st := &Status{
		StatusCode: http.StatusConflict,
		Code:       dataapiv1.ErrorCodePathExists,
		Message: fmt.Sprintf("Subdocument path '%s' already exists in '%s' in '%s/%s/%s'.",
			sdPath, docId, bucketName, scopeName, collectionName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s/content/{%s}",
			bucketName, scopeName, collectionName, docId, sdPath),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdPathTooBigStatus(baseErr error, sdPath string) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    fmt.Sprintf("Subdocument path '%s' is too long", sdPath),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdXattrUnknownVattrStatus(baseErr error, sdPath string) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    fmt.Sprintf("Subdocument path '%s' references an invalid virtual attribute.", sdPath),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdBadCombo(baseErr error) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    "Invalid subdocument combination specified.",
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewInvalidContentEncodingStatus(encoding dataapiv1.DocumentEncoding) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    fmt.Sprintf("Invalid content encoding '%s'.", encoding),
	}
	return st
}

func (e ErrorHandler) NewInvalidEtagFormatStatus(etag string) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    fmt.Sprintf("Invalid etag format '%s'.", etag),
	}
	return st
}

func (e ErrorHandler) NewZeroCasStatus() *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    "CAS value cannot be zero.",
	}
	return st
}

func (e ErrorHandler) NewDocCasMismatchStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *Status {
	st := &Status{
		StatusCode: http.StatusConflict,
		Code:       dataapiv1.ErrorCodeCasMismatch,
		Message: fmt.Sprintf("The specified CAS for '%s' in '%s/%s/%s' did not match.",
			docId, bucketName, scopeName, collectionName),
		Resource: fmt.Sprintf("/buckets/%s/scopes/%s/collections/%s/documents/%s",
			bucketName, scopeName, collectionName, docId),
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewTooFewOperationsError() *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    "Must specify at least one operation.",
	}
	return st
}

func (e ErrorHandler) NewInvalidOperationTypeError(opIndex int) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    fmt.Sprintf("Operation type not specified for operation at index %d.", opIndex),
	}
	return st
}

func (e ErrorHandler) NewInvalidStoreSemanticError() *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    "Invalid store semantic specified.",
	}
	return st
}

func (e ErrorHandler) NewInvalidSnappyValueError() *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    "Failed to snappy inflate value.",
	}
	return st
}

func (e ErrorHandler) NewSubDocMkDocSubDocOp(opIndex int) *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    fmt.Sprintf("Cannot execute replace semantic sub-document operations when store semantic implies document may not exist at index %d.", opIndex),
	}
	return st
}

func (e ErrorHandler) NewTouchMissingExpiryStatus() *Status {
	st := &Status{
		StatusCode: http.StatusBadRequest,
		Code:       dataapiv1.ErrorCodeInvalidArgument,
		Message:    "Expiry not specified in request body.",
	}
	return st
}
