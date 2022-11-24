package server_v1

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/couchbase/gocb/v2"
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
func tryAttachStatusDetails(st *status.Status, details ...protoiface.MessageV1) *status.Status {
	// try to attach the details
	if newSt, err := st.WithDetails(details...); err == nil {
		return newSt
	}

	// if we failed to attach the details, just return the original error
	return st
}

func tryAttachCbContext(st *status.Status, baseErr error) *status.Status {
	var kvErrCtx *gocb.KeyValueError
	if errors.As(baseErr, &kvErrCtx) {
		if kvErrCtx.Ref != "" {
			st = tryAttachStatusDetails(st, &epb.RequestInfo{
				RequestId: kvErrCtx.Ref,
			})
		}
	}
	return st
}

func newUnknownStatus(baseErr error) *status.Status {
	st := status.New(codes.Unknown, "An unknown error occurred.")
	return st
}

func newDocMissingStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	st := status.New(codes.NotFound,
		fmt.Sprintf("Document '%s' not found in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName))
	st = tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "document",
		ResourceName: fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, docId),
		Description:  "",
	})
	st = tryAttachCbContext(st, baseErr)
	return st
}

func newDocExistsStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	st := status.New(codes.AlreadyExists,
		fmt.Sprintf("Document '%s' already existed in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName))
	st = tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "document",
		ResourceName: fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, docId),
		Description:  "",
	})
	st = tryAttachCbContext(st, baseErr)
	return st
}

func newDocCasMismatchStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("The specified CAS for '%s' in '%s/%s/%s' did not match.",
			docId, bucketName, scopeName, collectionName))
	st = tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "CAS",
			Subject:     fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, docId),
			Description: "",
		}},
	})
	st = tryAttachCbContext(st, baseErr)
	return st
}

func newDocLockedStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("Cannot perform a write operation against locked document '%s' in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName))
	st = tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "LOCKED",
			Subject:     fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, docId),
			Description: "",
		}},
	})
	st = tryAttachCbContext(st, baseErr)
	return st
}

func newDocNotNumericContentStatus(baseErr error, bucketName, scopeName, collectionName, docId string) *status.Status {
	// TODO(brett19): We should improve this error message...
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("Document content type does not support increment/decrement for '%s' in '%s/%s/%s'.",
			docId, bucketName, scopeName, collectionName))
	st = tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "CONTENT_TYPE",
			Subject:     fmt.Sprintf("%s/%s/%s/%s", bucketName, scopeName, collectionName, docId),
			Description: "",
		}},
	})
	st = tryAttachCbContext(st, baseErr)
	return st
}

func newCollectionNoReadAccessStatus(baseErr error, bucketName, scopeName, collectionName string) *status.Status {
	st := status.New(codes.PermissionDenied,
		fmt.Sprintf("No permissions to read documents from '%s/%s/%s'.",
			bucketName, scopeName, collectionName))
	st = tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "collection",
		ResourceName: fmt.Sprintf("%s/%s/%s", bucketName, scopeName, collectionName),
		Description:  "",
	})
	st = tryAttachCbContext(st, baseErr)
	return st
}

func newCollectionNoWriteAccessStatus(baseErr error, bucketName, scopeName, collectionName string) *status.Status {
	st := status.New(codes.PermissionDenied,
		fmt.Sprintf("No permissions to write documents into '%s/%s/%s'.",
			bucketName, scopeName, collectionName))
	st = tryAttachStatusDetails(st, &epb.ResourceInfo{
		ResourceType: "collection",
		ResourceName: fmt.Sprintf("%s/%s/%s", bucketName, scopeName, collectionName),
		Description:  "",
	})
	st = tryAttachCbContext(st, baseErr)
	return st
}

func newSdPathNotFoundStatus(baseErr error, bucketName, scopeName, collectionName, docId, sdPath string) *status.Status {
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("Subdocument path '%s' was not found in '%s' in '%s/%s/%s'.",
			sdPath, docId, bucketName, scopeName, collectionName))
	st = tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "PATH_NOT_FOUND",
			Subject:     sdPath,
			Description: "",
		}},
	})
	st = tryAttachCbContext(st, baseErr)
	return st
}

func newSdPathMismatchStatus(baseErr error, bucketName, scopeName, collectionName, docId, sdPath string) *status.Status {
	st := status.New(codes.FailedPrecondition,
		fmt.Sprintf("Document structure implied by path '%s' did not match document '%s' in '%s/%s/%s'.",
			sdPath, docId, bucketName, scopeName, collectionName))
	st = tryAttachStatusDetails(st, &epb.PreconditionFailure{
		Violations: []*epb.PreconditionFailure_Violation{{
			Type:        "PATH_MISMATCH",
			Subject:     sdPath,
			Description: "",
		}},
	})
	st = tryAttachCbContext(st, baseErr)
	return st
}

func newSdPathEinvalStatus(baseErr error, sdPath string) *status.Status {
	st := status.New(codes.InvalidArgument,
		fmt.Sprintf("Invalid subdocument path syntax '%s'.", sdPath))
	// TODO(brett19): Probably should include invalid-argument error details.
	st = tryAttachCbContext(st, baseErr)
	return st
}

func cbGenericErrToPsStatus(err error) *status.Status {
	log.Printf("handling generic error: %+v", err)

	if errors.Is(err, context.Canceled) {
		return status.New(codes.Canceled, "The request was cancelled.")
	}

	return newUnknownStatus(err)
}
