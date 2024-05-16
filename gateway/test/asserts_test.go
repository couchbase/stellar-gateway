package test

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func assertStatus(t *testing.T, st *status.Status, expectedCode codes.Code) {
	if expectedCode == codes.OK && st == nil {
		return
	}

	if st == nil {
		t.Fatalf("expected rpc error status but got nil")
	}

	assert.Equal(t, expectedCode, st.Code())
}

func assertStatusDetails[T any](t *testing.T, st *status.Status, checkFn func(detail *T)) {
	var expectedDetail *T
	var foundDetails []string
	for _, detail := range st.Details() {
		foundDetails = append(foundDetails, fmt.Sprintf("%T", detail))

		if reflect.TypeOf(detail) == reflect.TypeOf(expectedDetail) {
			checkFn(detail.(*T))
			return
		}
	}

	t.Fatalf("expected status detail of %T, but instead found [%s]",
		expectedDetail, strings.Join(foundDetails, ", "))
}

func assertStatusProto(t *testing.T, st *spb.Status, expectedCode codes.Code) {
	assertStatus(t, status.FromProto(st), expectedCode)
}

func assertStatusProtoDetails[T any](t *testing.T, st *spb.Status, checkFn func(detail *T)) {
	assertStatusDetails(t, status.FromProto(st), checkFn)
}

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

	assert.Equal(t, expectedCode, errSt.Code())
}

func requireRpcSuccess(t *testing.T, resp interface{}, err error) {
	assertRpcStatus(t, err, codes.OK)
	require.NotNil(t, resp)
}

func assertRpcErrorDetails[T any](t *testing.T, err error, checkFn func(detail *T)) {
	if err == nil {
		t.Fatalf("expected rpc error status but got nil")
	}

	errSt, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected rpc error status, but got non-status error")
	}

	assertStatusDetails(t, errSt, checkFn)
}

func assertValidCas(t *testing.T, cas uint64) {
	assert.NotZero(t, cas)
}

func assertValidMutationToken(t *testing.T, token *kv_v1.MutationToken, bucketName string) {
	assert.NotNil(t, token)
	assert.NotEmpty(t, token.BucketName)
	if bucketName != "" {
		assert.Equal(t, bucketName, token.BucketName)
	}
	assert.NotZero(t, token.VbucketUuid)
	assert.NotZero(t, token.SeqNo)
}

func requireValidTimestamp(t *testing.T, ts *timestamppb.Timestamp) {
	require.NotNil(t, ts)
}

func requireRestSuccess(t *testing.T, resp *testHttpResponse) {
	require.NotNil(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode, fmt.Sprintf("status code was not 200 body::\n%s", string(resp.Body)))
}

func assertRestValidEtag(t *testing.T, resp *testHttpResponse) {
	etag := resp.Headers.Get("Etag")
	assert.NotEmpty(t, etag)
}

func assertRestValidMutationToken(t *testing.T, resp *testHttpResponse, bucketName string) {
	token := resp.Headers.Get("X-CB-MutationToken")
	assert.NotEmpty(t, token)

	// fmt.Sprintf("%s:%d:%08x:%d", bucketName, token.VbID, token.VbUuid, token.SeqNo)
	parts := strings.Split(token, ":")
	assert.Len(t, parts, 4)

	if len(parts) >= 4 {
		assert.NotEmpty(t, parts[0])
		assert.Equal(t, bucketName, parts[0])

		assert.NotEmpty(t, parts[1])

		assert.NotEmpty(t, parts[2])

		assert.NotEmpty(t, parts[3])
	}
}
