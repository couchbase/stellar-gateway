package server_v1

import (
	"fmt"
	"net/http"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/commonflags"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/stellar-gateway/dataapiv1"
)

func casToHttpEtag(cas uint64) string {
	return fmt.Sprintf("%08x", cas)
}

func timeToHttpTime(when time.Time) string {
	if when.IsZero() {
		return ""
	}

	return when.Format(time.RFC1123)
}

func httpTimeToGocbcorexExpiry(when string) (uint32, *Status) {
	if when == "0" || when == "" {
		return 0, nil
	}

	t, err := time.Parse(time.RFC1123, when)
	if err != nil {
		return 0, &Status{
			StatusCode: http.StatusBadRequest,
			Message:    "Invalid time format.",
		}
	}

	return uint32(t.Unix()), nil
}

func isoTimeToGocbcorexExpiry(when string) (uint32, *Status) {
	t, err := time.Parse(time.RFC3339, when)
	if err != nil {
		return 0, &Status{
			StatusCode: http.StatusBadRequest,
			Message:    "Invalid time format - expected ISO8601.",
		}
	}

	return uint32(t.Unix()), nil
}

func tokenFromGocbcorex(bucketName string, token gocbcorex.MutationToken) string {
	return fmt.Sprintf("%s:%d:%08x:%d", bucketName, token.VbID, token.VbUuid, token.SeqNo)
}

func durabilityLevelToMemdx(dl dataapiv1.DurabilityLevel) (memdx.DurabilityLevel, *Status) {
	switch dl {
	case dataapiv1.DurabilityLevelNone:
		return memdx.DurabilityLevelMajority, nil
	case dataapiv1.DurabilityLevelMajorityAndPersistOnMaster:
		return memdx.DurabilityLevelMajorityAndPersistToActive, nil
	case dataapiv1.DurabilityLevelPersistToMajority:
		return memdx.DurabilityLevelPersistToMajority, nil
	}

	return memdx.DurabilityLevel(0), &Status{
		StatusCode: http.StatusBadRequest,
		Message:    "Invalid durability level specified.",
	}
}

func flagsToHttpContentType(flags uint32) string {
	dataType, _ := commonflags.Decode(flags)

	switch dataType {
	case commonflags.JSONType:
		return "application/json"
	case commonflags.StringType:
		return "text/plain"
	}

	return "application/octet-stream"
}

func lookupInOperationToMemdx(dl *dataapiv1.LookupInOperationType) (memdx.LookupInOpType, bool) {
	if dl == nil {
		return memdx.LookupInOpType(0), false
	}

	switch *dl {
	case dataapiv1.LookupInOperationTypeGet:
		return memdx.LookupInOpTypeGet, true
	case dataapiv1.LookupInOperationTypeExists:
		return memdx.LookupInOpTypeExists, true
	case dataapiv1.LookupInOperationTypeGetCount:
		return memdx.LookupInOpTypeGetCount, true
	}

	return memdx.LookupInOpType(0), false
}

func mutateInOperationToMemdx(dl *dataapiv1.MutateInOperationType) (memdx.MutateInOpType, bool) {
	if dl == nil {
		return memdx.MutateInOpType(0), false
	}

	switch *dl {
	case dataapiv1.MutateInOperationTypeDictAdd:
		return memdx.MutateInOpTypeDictAdd, true
	case dataapiv1.MutateInOperationTypeDictSet:
		return memdx.MutateInOpTypeDictSet, true
	case dataapiv1.MutateInOperationTypeDelete:
		return memdx.MutateInOpTypeDelete, true
	case dataapiv1.MutateInOperationTypeReplace:
		return memdx.MutateInOpTypeReplace, true
	case dataapiv1.MutateInOperationTypeArrayPushLast:
		return memdx.MutateInOpTypeArrayPushLast, true
	case dataapiv1.MutateInOperationTypeArrayPushFirst:
		return memdx.MutateInOpTypeArrayPushFirst, true
	case dataapiv1.MutateInOperationTypeArrayInsert:
		return memdx.MutateInOpTypeArrayInsert, true
	case dataapiv1.MutateInOperationTypeArrayAddUnique:
		return memdx.MutateInOpTypeArrayAddUnique, true
	case dataapiv1.MutateInOperationTypeCounter:
		return memdx.MutateInOpTypeCounter, true
	}

	return memdx.MutateInOpType(0), false
}
