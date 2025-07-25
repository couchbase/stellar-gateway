package server_v1

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/couchbase/gocbcorex"
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

func parseStringToGocbcorexExpiry(val string) (uint32, *Status) {
	if val == "0" || val == "" {
		return 0, nil
	}

	t, err := time.Parse(time.RFC1123, val)
	if err != nil {
		var parseErr *time.ParseError
		if errors.As(err, &parseErr) {
			d, err := time.ParseDuration(val)
			if err == nil {
				return uint32(time.Now().Add(d).Unix()), nil
			}
		}

		return 0, &Status{
			StatusCode: http.StatusBadRequest,
			Message:    "Invalid time format - expected ISO8601 format, 0, or a Go style duration.",
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
		return memdx.DurabilityLevelNone, nil
	case dataapiv1.DurabilityLevelMajority:
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

func readerHasBytes(r io.Reader) bool {
	buf := make([]byte, 1)
	n, _ := r.Read(buf)
	return n > 0
}
