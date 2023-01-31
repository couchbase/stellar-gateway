package test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/couchbase/stellar-nebula/genproto/kv_v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func assertRpcStatus(t *testing.T, err error, expectedCode codes.Code) {
	if expectedCode == codes.OK && err == nil {
		return
	}

	if err == nil {
		t.Fatalf("expected rpc error status but got nil")
	}

	errSt, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected rpc error status, but got non-status error")
	}

	assert.Equal(t, errSt.Code(), expectedCode)
}

func assertRpcSuccess(t *testing.T, resp interface{}, err error) {
	assertRpcStatus(t, err, codes.OK)
	assert.NotNil(t, resp)
}

func assertRpcErrorDetails[T any](t *testing.T, err error, checkFn func(detail *T)) {
	if err == nil {
		t.Fatalf("expected rpc error status but got nil")
	}

	errSt, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected rpc error status, but got non-status error")
	}

	var expectedDetail *T
	var foundDetails []string
	for _, detail := range errSt.Details() {
		foundDetails = append(foundDetails, fmt.Sprintf("%T", detail))

		if reflect.TypeOf(detail) == reflect.TypeOf(expectedDetail) {
			checkFn(detail.(*T))
			return
		}
	}

	t.Fatalf("expected rpc error status detail of %T, but instead found [%s]",
		expectedDetail, strings.Join(foundDetails, ", "))
}

func assertValidCas(t *testing.T, cas uint64) {
	assert.NotZero(t, cas)
}

func assertValidMutationToken(t *testing.T, token *kv_v1.MutationToken, bucketName string) {
	assert.NotNil(t, token)
	assert.NotEmpty(t, token.BucketName)
	if bucketName != "" {
		assert.Equal(t, token.BucketName, bucketName)
	}
	assert.NotZero(t, token.VbucketId)
	assert.NotZero(t, token.VbucketUuid)
	assert.NotZero(t, token.SeqNo)
}

func assertValidTimestamp(t *testing.T, ts *timestamppb.Timestamp) {
	assert.NotNil(t, ts)
}