package server_v1

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/gocbcorex/cbsearchx"
	"github.com/couchbase/gocbcorex/memdx"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/runtime/protoiface"

	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
)

/*
INVALID_ARGUMENT - The client sent a value which could never be considered correct.
FAILED_PRECONDITION - Something is in a state making the request invalid, retrying _COULD_ help.
OUT_OF_RANGE - More specific version of FAILED_PRECONDITION, useful to allow clients to iterate results until hitting this.
UNAUTHENTICATED - Occurs when credentials are not sent for something that must be authenticated.
PERMISSION_DENIED - Credentials were sent, but are insufficient.
NOT_FOUND - More specific version of FAILED_PRECONDITION where the document must exist, but does not.
ABORTED - Operation was running but was unambiguously aborted, must be retried at a higher level than at the request level.
ALREADY_EXISTS - More specific version of FAILED_PRECONDITION where the document must not exist, but does.
RESOURCE_EXHAUSTED - Indicates that some transient resource was exhausted, this could be quotas, limits, etc...  Implies retriability.
CANCELLED - Happens when a client explicitly cancels an operation.
DATA_LOSS - Indicates that data was lost unexpectedly and the operation cannot succeed.
UNKNOWN - Specified when no information about the cause of the error is available, otherwise INTERNAL should be used.
INTERNAL - Indicates any sort of error where the protocol wont provide parseable details to the client.
NOT_IMPLEMENTED - Indicates that a feature is not implemented, either a whole RPC, or possibly an option.
UNAVAILABLE - Cannot access the resource for some reason, clients can generate this if the server isn't available.
DEADLINE_EXCEEDED - Timeout occurred while processing.
*/

type ErrorHandler struct {
	Logger *zap.Logger
	Debug  bool
}

func (e ErrorHandler) tryAttachStatusDetails(st *status.Status, details ...protoiface.MessageV1) *status.Status {
	// try to attach the details
	if newSt, err := st.WithDetails(details...); err == nil {
		return newSt
	}

	// if we failed to attach the details, just return the original error
	return st
}

func (e ErrorHandler) tryAttachExtraContext(st *status.Status, baseErr error) *status.Status {
	if baseErr == nil {
		return st
	}

	var memdSrvErr *memdx.ServerErrorWithContext
	if errors.As(baseErr, &memdSrvErr) {
		parsedCtx := memdSrvErr.ParseContext()
		if parsedCtx.Ref != "" {
			st = e.tryAttachStatusDetails(st, &epb.RequestInfo{
				RequestId: parsedCtx.Ref,
			})
		}
	}

	if e.Debug {
		st = e.tryAttachStatusDetails(st, &epb.DebugInfo{
			Detail: baseErr.Error(),
		})
	}

	return st
}

func (e ErrorHandler) NewInternalStatus() *status.Status {
	st := status.New(codes.Internal, "An internal error occurred.")
	return st
}

func (e ErrorHandler) NewUnknownStatus(baseErr error) *status.Status {
	var memdErr *memdx.ServerError
	if errors.As(baseErr, &memdErr) {
		st := status.New(codes.Unknown, fmt.Sprintf("An unknown memcached error occurred (status: %d).", memdErr.Status))
		st = e.tryAttachExtraContext(st, baseErr)
		return st
	}

	var queryErr *cbqueryx.ServerErrors
	if errors.As(baseErr, &queryErr) {
		var queryErrDescs []string
		for _, querySubErr := range queryErr.Errors {
			queryErrDescs = append(queryErrDescs, fmt.Sprintf("%d - %s", querySubErr.Code, querySubErr.Msg))
		}

		st := status.New(codes.Unknown,
			fmt.Sprintf("An unknown query error occurred (descs: %s).", strings.Join(queryErrDescs, "; ")))
		st = e.tryAttachExtraContext(st, baseErr)
		return st
	}

	var searchErr *cbsearchx.ServerError
	if errors.As(baseErr, &searchErr) {
		st := status.New(codes.Unknown,
			fmt.Sprintf("An unknown search error occurred (status: %d).", searchErr.StatusCode))
		st = e.tryAttachExtraContext(st, baseErr)
		return st
	}

	var serverErr *cbmgmtx.ServerError
	if errors.As(baseErr, &serverErr) {
		st := status.New(codes.Unknown,
			fmt.Sprintf("An unknown server error occurred (status: %d).", serverErr.StatusCode))
		st = e.tryAttachExtraContext(st, baseErr)
		return st
	}

	st := status.New(codes.Unknown, "An unknown error occurred.")
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewBucketMissingStatus(baseErr error, bucketName string) *status.Status {
	st := status.New(codes.NotFound,
		fmt.Sprintf("Bucket '%s' was not found.",
			bucketName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "bucket",
		ResourceName: bucketName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewBucketExistsStatus(baseErr error, bucketName string) *status.Status {
	st := status.New(codes.AlreadyExists,
		fmt.Sprintf("Bucket '%s' already existed.",
			bucketName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "bucket",
		ResourceName: bucketName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewBucketFlushDisabledStatus(baseErr error, bucketName string) *status.Status {
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("Flush is disabled for bucket '%s'.",
			bucketName))
	st = e.tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "FLUSH_DISABLED",
			Subject:     bucketName,
			Description: "",
		}},
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewBucketInvalidArgStatus(baseErr error, msg string, bucketName string) *status.Status {
	if msg == "" {
		msg = "invalid argument"
	}
	st := status.New(codes.InvalidArgument, msg)

	var sErr *cbmgmtx.ServerInvalidArgError
	if errors.As(baseErr, &sErr) {
		st = status.New(codes.InvalidArgument, fmt.Sprintf("invalid argument: %s - %s", sErr.Argument, sErr.Reason))
	}

	if baseErr != nil {
		st = e.tryAttachExtraContext(st, baseErr)
	}

	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "bucket",
		ResourceName: bucketName,
		Description:  "",
	})
	return st
}

func (e ErrorHandler) NewBucketAccessDeniedStatus(baseErr error, bucketName string) *status.Status {
	msg := "No permissions to perform bucket management operation."
	st := status.New(codes.PermissionDenied, msg)

	st = e.tryAttachStatusDetails(
		st, &epb.ResourceInfo{
			ResourceType: "bucket",
			ResourceName: bucketName,
			Description:  "",
		})
	return st
}

func (e ErrorHandler) NewCollectionInvalidArgStatus(baseErr error, msg string, bucket, scope, collection string) *status.Status {
	if msg == "" {
		msg = "invalid argument"
	}
	st := status.New(codes.InvalidArgument, msg)

	var sErr *cbmgmtx.ServerInvalidArgError
	if errors.As(baseErr, &sErr) {
		st = status.New(codes.InvalidArgument, fmt.Sprintf("invalid argument: %s - %s", sErr.Argument, sErr.Reason))
	}

	if baseErr != nil {
		st = e.tryAttachExtraContext(st, baseErr)
	}

	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "collection",
		ResourceName: fmt.Sprintf("%s/%s/%s", bucket, scope, collection),
		Description:  "",
	})
	return st
}

func (e ErrorHandler) NewScopeMissingStatus(baseErr error, bucketName, scopeName string) *status.Status {
	st := status.New(codes.NotFound,
		fmt.Sprintf("Scope '%s' not found in '%s'.",
			scopeName, bucketName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "scope",
		ResourceName: fmt.Sprintf("%s/%s", bucketName, scopeName),
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewCollectionMissingStatus(baseErr error, bucketName, scopeName, collectionName string) *status.Status {
	st := status.New(codes.NotFound,
		fmt.Sprintf("Collection '%s' not found in '%s/%s'.",
			collectionName, bucketName, scopeName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "collection",
		ResourceName: fmt.Sprintf("%s/%s/%s", bucketName, scopeName, collectionName),
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewScopeExistsStatus(baseErr error, bucketName, scopeName string) *status.Status {
	st := status.New(codes.AlreadyExists,
		fmt.Sprintf("Scope '%s' already existed in '%s'.",
			scopeName, bucketName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "scope",
		ResourceName: fmt.Sprintf("%s/%s", bucketName, scopeName),
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewCollectionExistsStatus(baseErr error, bucketName, scopeName, collectionName string) *status.Status {
	st := status.New(codes.AlreadyExists,
		fmt.Sprintf("Collection '%s' already existed in '%s/%s'.",
			collectionName, bucketName, scopeName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "collection",
		ResourceName: fmt.Sprintf("%s/%s/%s", bucketName, scopeName, collectionName),
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewQueryIndexMissingStatus(baseErr error, indexName, bucketName, scopeName, collectionName string) *status.Status {
	var path string
	if bucketName != "" {
		path = bucketName
		if scopeName != "" {
			path = path + "/" + scopeName
			if collectionName != "" {
				path = path + "/" + collectionName
			}
		}
	}

	var msg string
	if indexName == "" {
		msg = "Query index not found."
	} else {
		msg = fmt.Sprintf("Query index '%s' not found in '%s'.", indexName, path)
	}
	st := status.New(codes.NotFound, msg)
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "queryindex",
		ResourceName: indexName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewQueryIndexExistsStatus(baseErr error, indexName, bucketName, scopeName, collectionName string) *status.Status {
	var path string
	if bucketName != "" {
		path = bucketName
		if scopeName != "" {
			path = path + "/" + scopeName
			if collectionName != "" {
				path = path + "/" + collectionName
			}
		}
	}

	var msg string
	if indexName == "" {
		msg = "Query index already existed."
	} else {
		msg = fmt.Sprintf("Query index '%s' already existed in  '%s'.", indexName, path)
	}
	st := status.New(codes.AlreadyExists, msg)
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "queryindex",
		ResourceName: indexName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewQueryIndexNotBuildingStatus(baseErr error, bucketName, scopeName, collectionName, indexName string) *status.Status {
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("Cannot wait for index '%s' in '%s/%s/%s' to be ready as it is still deferred.",
			indexName, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "NOT_BUILDING",
			Subject:     fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, indexName),
			Description: "",
		}},
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewQueryIndexAuthenticationFailureStatus(baseErr error, bucketName, scopeName, collectionName string) *status.Status {
	var path string
	var resource string
	if bucketName != "" {
		path = bucketName
		resource = "bucket"
		if scopeName != "" {
			path = path + "/" + scopeName
			resource = "scope"
			if collectionName != "" {
				path = path + "/" + collectionName
				resource = "collection"
			}
		}
	}

	msg := fmt.Sprintf("Insufficient permissions to perform query index operation against %s.", path)
	st := status.New(codes.PermissionDenied, msg)
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: resource,
		ResourceName: path,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewQueryIndexInvalidArgumentStatus(baseErr error, indexName, msg string) *status.Status {
	st := status.New(codes.InvalidArgument, msg)
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "queryindex",
		ResourceName: indexName,
		Description:  "",
	})

	if baseErr != nil {
		e.tryAttachExtraContext(st, baseErr)
	}

	return st
}

func (e ErrorHandler) NewSearchServiceNotAvailableStatus(baseErr error, indexName string) *status.Status {
	st := status.New(codes.Unimplemented, "Search service is not available.")
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "searchIndex",
		ResourceName: indexName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)

	return st
}

func (e ErrorHandler) NewSearchIndexMissingStatus(baseErr error, indexName string) *status.Status {
	st := status.New(codes.NotFound,
		fmt.Sprintf("Search index '%s' not found.",
			indexName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "searchindex",
		ResourceName: indexName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSearchIndexExistsStatus(baseErr error, indexName string) *status.Status {
	st := status.New(codes.AlreadyExists,
		fmt.Sprintf("Search index '%s' already existed.",
			indexName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "searchindex",
		ResourceName: indexName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSearchIndexNameInvalidStatus(baseErr error, indexName string) *status.Status {
	st := status.New(codes.InvalidArgument,
		fmt.Sprintf("Name for search index '%s' is invalid.",
			indexName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "searchindex",
		ResourceName: indexName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSearchIndexNameTooLongStatus(baseErr error, indexName string) *status.Status {
	st := status.New(codes.InvalidArgument,
		fmt.Sprintf("Search index name '%s' is too long.",
			indexName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "searchindex",
		ResourceName: indexName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSearchIndexNameEmptyStatus(baseErr error) *status.Status {
	st := status.New(codes.InvalidArgument, "Must specify an index name when performing this operation.")
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "searchindex",
		ResourceName: "",
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewUnknownSearchIndexTypeStatus(baseErr error, indexName string, indexType string) *status.Status {
	st := status.New(codes.InvalidArgument,
		fmt.Sprintf("Search index type '%s' is unknown.",
			indexType))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "searchindex",
		ResourceName: indexName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSearchUUIDMismatchStatus(baseErr error, indexName string) *status.Status {
	st := status.New(codes.Aborted,
		fmt.Sprintf("Search index '%s' already existed with a different UUID.",
			indexName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "searchindex",
		ResourceName: indexName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewOnlyBucketOrScopeSetStatus(baseErr error, indexName string) *status.Status {
	st := status.New(codes.InvalidArgument, "Must specify both or neither of scope and bucket names.")
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "searchindex",
		ResourceName: indexName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSearchIndexNotReadyStatus(baseErr error, indexName string) *status.Status {
	st := status.New(codes.Unavailable, "Search index is still being built, try again later.")
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "searchindex",
		ResourceName: indexName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewIncorrectSearchSourceTypeStatus(baseErr error, indexName string, sourceType *string) *status.Status {
	var sT string
	if sourceType != nil {
		sT = *sourceType
	}
	st := status.New(codes.InvalidArgument, fmt.Sprintf("'%s' is not a valid source type.", sT))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "searchindex",
		ResourceName: indexName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSearchSourceNotFoundStatus(baseErr error, indexName string, sourceName *string) *status.Status {
	var sN string
	if sourceName != nil {
		sN = *sourceName
	}
	st := status.New(codes.NotFound, fmt.Sprintf("Source bucket '%s' for search index was not found.", sN))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "bucket",
		ResourceName: indexName,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSearchIndexAuthenticationFailureStatus(baseErr error, bucketName, scopeName *string) *status.Status {
	var path string
	var resource string
	if bucketName != nil {
		path = *bucketName
		resource = "bucket"
		if scopeName != nil {
			path = path + "/" + *scopeName
			resource = "scope"
		}
	}

	msg := fmt.Sprintf("Insufficient permissions to perform search index operation against %s.", path)
	st := status.New(codes.PermissionDenied, msg)
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: resource,
		ResourceName: path,
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewDocMissingStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	st := status.New(codes.NotFound,
		fmt.Sprintf("Document '%s' not found in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "document",
		ResourceName: fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, docId),
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewDocExistsStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	st := status.New(codes.AlreadyExists,
		fmt.Sprintf("Document '%s' already existed in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "document",
		ResourceName: fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, docId),
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewDocCasMismatchStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	st := status.New(codes.Aborted,
		fmt.Sprintf("The specified CAS for '%s' in '%s/%s/%s' did not match.",
			docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.ErrorInfo{
		Reason: "CAS_MISMATCH",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewDocConflictStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	st := status.New(codes.Aborted,
		fmt.Sprintf("Conflict resolution rejected '%s' in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.ErrorInfo{
		Reason: "DOC_NEWER",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewZeroCasStatus() *status.Status {
	st := status.New(codes.InvalidArgument, "CAS value cannot be zero.")
	return st
}

func (e ErrorHandler) NewDocLockedStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("Cannot perform a write operation against locked document '%s' in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "LOCKED",
			Subject:     fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, docId),
			Description: "",
		}},
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewDocNotLockedStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("Cannot unlock an unlocked document '%s' in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "NOT_LOCKED",
			Subject:     fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, docId),
			Description: "",
		}},
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewDocNotNumericStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	var st *status.Status
	st = status.New(codes.FailedPrecondition,
		fmt.Sprintf("Cannot perform counter operation on non-numeric document '%s' in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "DOC_NOT_NUMERIC",
			Subject:     fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, docId),
			Description: "",
		}},
	})

	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewValueTooLargeStatus(baseErr error, bucketName, scopeName, collectionName, docId string,
	isExpandingValue bool) *status.Status {
	var st *status.Status
	if isExpandingValue {
		st = status.New(codes.FailedPrecondition,
			fmt.Sprintf("Updated value '%s' made value too large in '%s/%s/%s'.",
				docId, bucketName, scopeName, collectionName))
		st = e.tryAttachStatusDetails(st, &epb.PreconditionFailure{
			Violations: []*epb.PreconditionFailure_Violation{{
				Type:        "VALUE_TOO_LARGE",
				Subject:     fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, docId),
				Description: "",
			}},
		})
	} else {
		st = status.New(codes.InvalidArgument,
			fmt.Sprintf("Value '%s' for new document was too large in '%s/%s/%s'.",
				docId, bucketName, scopeName, collectionName))
	}
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewDurabilityImpossibleStatus(baseErr error, bucketName string) *status.Status {
	var st *status.Status
	st = status.New(codes.FailedPrecondition,
		fmt.Sprintf("Not enough servers to use this durability level on '%s' bucket.",
			bucketName))
	st = e.tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "DURABILITY_IMPOSSIBLE",
			Subject:     fmt.Sprintf("%ss", bucketName),
			Description: "",
		}},
	})

	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSyncWriteAmbiguousStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	return status.New(codes.DeadlineExceeded,
		fmt.Sprintf("Sync write operation on '%s' in '%s/%s/%s' timed out.",
			docId, bucketName, scopeName, collectionName))
}

func (e ErrorHandler) NewCollectionNoReadAccessStatus(baseErr error, bucketName, scopeName, collectionName string) *status.Status {
	st := status.New(codes.PermissionDenied,
		fmt.Sprintf("No permissions to read documents from '%s/%s/%s'.",
			bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "collection",
		ResourceName: fmt.Sprintf("%s/%s/%s", bucketName, scopeName, collectionName),
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewCollectionNoWriteAccessStatus(baseErr error, bucketName, scopeName, collectionName string) *status.Status {
	st := status.New(codes.PermissionDenied,
		fmt.Sprintf("No permissions to write documents into '%s/%s/%s'.",
			bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "collection",
		ResourceName: fmt.Sprintf("%s/%s/%s", bucketName, scopeName, collectionName),
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdDocTooDeepStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("Document '%s' JSON was too deep to parse in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "DOC_TOO_DEEP",
			Subject:     fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, docId),
			Description: "",
		}},
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdDocNotJsonStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("Document '%s' was not JSON in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "DOC_NOT_JSON",
			Subject:     fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, docId),
			Description: "",
		}},
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdPathNotFoundStatus(baseErr error, bucketName, scopeName, collectionName, docId, sdPath string) *status.Status {
	st := status.New(codes.NotFound,
		fmt.Sprintf("Subdocument path '%s' was not found in '%s' in '%s/%s/%s'.",
			sdPath, docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "path",
		ResourceName: fmt.Sprintf("%s/%s/%s/%s/%s", bucketName, scopeName, collectionName, docId, sdPath),
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdPathExistsStatus(baseErr error, bucketName, scopeName, collectionName, docId, sdPath string) *status.Status {
	st := status.New(codes.AlreadyExists,
		fmt.Sprintf("Subdocument path '%s' already existed in '%s' in '%s/%s/%s'.",
			sdPath, docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "path",
		ResourceName: fmt.Sprintf("%s/%s/%s/%s/%s", bucketName, scopeName, collectionName, docId, sdPath),
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdPathMismatchStatus(baseErr error, bucketName, scopeName, collectionName, docId, sdPath string) *status.Status {
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("Document structure implied by path '%s' did not match document '%s' in '%s/%s/%s'.",
			sdPath, docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "PATH_MISMATCH",
			Subject:     sdPath,
			Description: "",
		}},
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdPathTooBigStatus(baseErr error, sdPath string) *status.Status {
	st := status.New(codes.InvalidArgument,
		fmt.Sprintf("Subdocument path '%s' is too long", sdPath))
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdBadValueStatus(baseErr error, sdPath string) *status.Status {
	st := status.New(codes.InvalidArgument,
		fmt.Sprintf("Subdocument operation content for path '%s' would invalidate the JSON if added to the document.",
			sdPath))
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdValueOutOfRangeStatus(baseErr error, sdPath string) *status.Status {
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("Counter operation content for path '%s' would put the JSON value out of range.",
			sdPath))
	st = e.tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "VALUE_OUT_OF_RANGE",
			Subject:     sdPath,
			Description: "",
		}},
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdBadRangeStatus(baseErr error, bucketName, scopeName, collectionName, docId, sdPath string) *status.Status {
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("The value at path '%s' is out of the valid range in document '%s' in '%s/%s/%s'.",
			sdPath, docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "PATH_VALUE_OUT_OF_RANGE",
			Subject:     sdPath,
			Description: "",
		}},
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdBadDeltaStatus(baseErr error, sdPath string) *status.Status {
	st := status.New(codes.InvalidArgument,
		fmt.Sprintf("Subdocument counter delta for path '%s' was invalid.  Delta must be a non-zero number within the range of an 64-bit signed integer.",
			sdPath))
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdValueTooDeepStatus(baseErr error, sdPath string) *status.Status {
	st := status.New(codes.InvalidArgument,
		fmt.Sprintf("Subdocument operation content for path '%s' was too deep to parse.",
			sdPath))
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdXattrUnknownVattrStatus(baseErr error, sdPath string) *status.Status {
	st := status.New(codes.InvalidArgument,
		fmt.Sprintf("Subdocument path '%s' references an invalid virtual attribute.", sdPath))
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdBadCombo(baseErr error) *status.Status {
	st := status.New(codes.InvalidArgument, "Invalid subdocument combination specified.")
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewSdPathInvalidStatus(baseErr error, sdPath string) *status.Status {
	st := status.New(codes.InvalidArgument,
		fmt.Sprintf("Invalid subdocument path syntax '%s'.", sdPath))
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewInvalidSnappyValueError() *status.Status {
	st := status.New(codes.InvalidArgument, "Failed to snappy inflate value.")
	return st
}

func (e ErrorHandler) NewIllogicalCounterExpiry() *status.Status {
	st := status.New(codes.InvalidArgument,
		"Expiry cannot be set when the document does not exist and Initial is not set.  Expiry is only applied to new documents that are created.")
	return st
}

func (e ErrorHandler) NewUnsupportedFieldStatus(fieldPath string) *status.Status {
	st := status.New(codes.Unimplemented,
		fmt.Sprintf("The '%s' field is not currently supported", fieldPath))
	return st
}

func (e ErrorHandler) NewInvalidAuthHeaderStatus(baseErr error) *status.Status {
	st := status.New(codes.InvalidArgument, "Invalid authorization header format.")
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewNoAuthStatus() *status.Status {
	st := status.New(codes.Unauthenticated, "You must send authentication to use this endpoint.")
	return st
}

func (e ErrorHandler) NewInvalidCredentialsStatus() *status.Status {
	st := status.New(codes.PermissionDenied, "Your username or password is invalid.")
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "user",
		ResourceName: "",
		Description:  "",
	})
	return st
}

func (e ErrorHandler) NewInvalidCertificateStatus() *status.Status {
	st := status.New(codes.PermissionDenied, "Your certificate is invalid.")
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "user",
		ResourceName: "",
		Description:  "",
	})
	return st
}

func (e ErrorHandler) NewUnexpectedAuthTypeStatus() *status.Status {
	st := status.New(codes.InvalidArgument, "Unexpected auth type.")
	return st
}

func (e ErrorHandler) NewInvalidQueryStatus(baseErr error, queryErrStr string) *status.Status {
	st := status.New(codes.InvalidArgument,
		fmt.Sprintf("Query parsing failed: %s", queryErrStr))
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewWriteInReadOnlyQueryStatus(baseErr error) *status.Status {
	st := status.New(codes.InvalidArgument,
		"Write statements cannot be used in a read-only query")
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewQueryNoAccessStatus(baseErr error) *status.Status {
	st := status.New(codes.PermissionDenied,
		"No permissions to query documents.")
	st = e.tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "user",
		ResourceName: "",
		Description:  "",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewNeedIndexFieldsStatus() *status.Status {
	st := status.New(codes.InvalidArgument,
		"You must specify fields when creating a new index.")
	return st
}

func (e ErrorHandler) NewUnavailableStatus(err error) *status.Status {
	st := status.New(codes.Unavailable,
		"One of the underlying services were not available.")
	e.tryAttachExtraContext(st, err)
	return st
}

func (e ErrorHandler) NewGenericStatus(err error) *status.Status {
	// we do not attach context in these cases, since they almost always don't actually
	// make it back to the client to be processed anyways.
	if errors.Is(err, context.Canceled) {
		e.Logger.Debug("handling canceled operation error", zap.Error(err))

		return status.New(codes.Canceled, "The request was cancelled.")
	} else if errors.Is(err, context.DeadlineExceeded) {
		e.Logger.Debug("handling deadline exceeded operation error", zap.Error(err))

		return status.New(codes.DeadlineExceeded, "The request deadline was exceeded.")
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

func (e ErrorHandler) NewInvalidKeyLengthStatus(key string) *status.Status {
	st := status.New(
		codes.InvalidArgument,
		fmt.Sprintf("Length of document key '%s' must be between 1 and 251 characters.", key))
	return st
}

func (e ErrorHandler) NewVbUuidDivergenceStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	st := status.New(codes.Aborted,
		fmt.Sprintf("The specified vbuuid for '%s' in '%s/%s/%s' did not match.",
			docId, bucketName, scopeName, collectionName))
	st = e.tryAttachStatusDetails(st, &epb.ErrorInfo{
		Reason: "VBUUID_MISMATCH",
	})
	st = e.tryAttachExtraContext(st, baseErr)
	return st
}

func (e ErrorHandler) NewUnimplementedServerVersionStatus() *status.Status {
	st := status.New(
		codes.Unimplemented,
		"The requested feature is not available on this server version.")
	return st
}
