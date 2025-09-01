package test

import (
	"fmt"
	"net/http"
	"time"

	"github.com/couchbase/stellar-gateway/testutils"
)

func (s *GatewayOpsTestSuite) TestDapiDurabilityImpossible() {
	testutils.SkipIfNoDinoCluster(s.T())

	dino := testutils.StartDinoTesting(s.T(), false)

	nodes := testutils.GetTestNodes(s.T())
	for _, node := range nodes {
		if !node.IsOrchestrator {
			dino.RemoveNode(node.Hostname)
		}
	}

	// BUG(ING-1294) - ops can hang when performed immediately after node removal
	time.Sleep(time.Second * 10)

	type testCase struct {
		name string
		fn   func() *testHttpResponse
	}

	tests := []testCase{
		{
			name: "Post",
			fn: func() *testHttpResponse {
				docId := s.randomDocId()

				return s.sendTestHttpRequest(&testHttpRequest{
					Method: http.MethodPost,
					Path: fmt.Sprintf(
						"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
						s.bucketName, s.scopeName, s.collectionName, docId,
					),
					Headers: map[string]string{
						"Authorization":        s.basicRestCreds,
						"X-CB-Flags":           fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
						"X-CB-DurabilityLevel": "Majority",
					},
					Body: TEST_CONTENT,
				})
			},
		},
		{
			name: "Put",
			fn: func() *testHttpResponse {
				docId := s.randomDocId()
				return s.sendTestHttpRequest(&testHttpRequest{
					Method: http.MethodPut,
					Path: fmt.Sprintf(
						"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
						s.bucketName, s.scopeName, s.collectionName, docId,
					),
					Headers: map[string]string{
						"Authorization":        s.basicRestCreds,
						"X-CB-Flags":           fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
						"X-CB-DurabilityLevel": "Majority",
					},
					Body: TEST_CONTENT,
				})
			},
		},
		{
			name: "Replace",
			fn: func() *testHttpResponse {
				docId := s.randomDocId()
				return s.sendTestHttpRequest(&testHttpRequest{
					Method: http.MethodPut,
					Path: fmt.Sprintf(
						"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
						s.bucketName, s.scopeName, s.collectionName, docId,
					),
					Headers: map[string]string{
						"Authorization":        s.basicRestCreds,
						"If-Match":             "*",
						"X-CB-Flags":           fmt.Sprintf("%d", TEST_CONTENT_FLAGS),
						"X-CB-DurabilityLevel": "Majority",
					},
					Body: TEST_CONTENT,
				})
			},
		},
		{
			name: "Delete",
			fn: func() *testHttpResponse {
				docId := s.testDocId()
				return s.sendTestHttpRequest(&testHttpRequest{
					Method: http.MethodDelete,
					Path: fmt.Sprintf(
						"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s",
						s.bucketName, s.scopeName, s.collectionName, docId,
					),
					Headers: map[string]string{
						"Authorization":        s.basicRestCreds,
						"X-CB-DurabilityLevel": "Majority",
					},
					Body: nil,
				})
			},
		},
		{
			name: "Increment",
			fn: func() *testHttpResponse {
				docId := s.binaryDocId([]byte("5"))
				return s.sendTestHttpRequest(&testHttpRequest{
					Method: http.MethodPost,
					Path: fmt.Sprintf(
						"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s/increment",
						s.bucketName, s.scopeName, s.collectionName, docId,
					),
					Headers: map[string]string{
						"Authorization":        s.basicRestCreds,
						"X-CB-DurabilityLevel": "Majority",
					},
				})
			},
		},
		{
			name: "Decrement",
			fn: func() *testHttpResponse {
				docId := s.binaryDocId([]byte("5"))
				return s.sendTestHttpRequest(&testHttpRequest{
					Method: http.MethodPost,
					Path: fmt.Sprintf(
						"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s/decrement",
						s.bucketName, s.scopeName, s.collectionName, docId,
					),
					Headers: map[string]string{
						"Authorization":        s.basicRestCreds,
						"X-CB-DurabilityLevel": "Majority",
					},
				})
			},
		},
		{
			name: "MutateIn",
			fn: func() *testHttpResponse {
				docId := s.binaryDocId([]byte(`{
					"num":14,
					"rep":16,
					"arr":[3,6,9,12],
					"ctr":3,
					"rem":true
				}`))
				return s.sendTestHttpRequest(&testHttpRequest{
					Method: http.MethodPost,
					Path: fmt.Sprintf(
						"/v1.alpha/buckets/%s/scopes/%s/collections/%s/documents/%s/mutate",
						s.bucketName, s.scopeName, s.collectionName, docId,
					),
					Headers: map[string]string{
						"Authorization":        s.basicRestCreds,
						"X-CB-DurabilityLevel": "Majority",
					},
					Body: []byte(`{"operations":[
						{"operation":"DictSet","path":"num","value": 42}
					]}`),
				})
			},
		},
		{
			name: "Append",
			fn: func() *testHttpResponse {
				docId := s.binaryDocId([]byte(`abcde`))
				return s.sendTestHttpRequest(&testHttpRequest{
					Method: http.MethodPost,
					Path: fmt.Sprintf(
						"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s/append",
						s.bucketName, s.scopeName, s.collectionName, docId,
					),
					Headers: map[string]string{
						"Authorization":        s.basicRestCreds,
						"X-CB-DurabilityLevel": "Majority",
					},
					Body: []byte(`fghi`),
				})
			},
		},
		{
			name: "Prepend",
			fn: func() *testHttpResponse {
				docId := s.binaryDocId([]byte(`fghi`))
				return s.sendTestHttpRequest(&testHttpRequest{
					Method: http.MethodPost,
					Path: fmt.Sprintf(
						"/v1/buckets/%s/scopes/%s/collections/%s/documents/%s/prepend",
						s.bucketName, s.scopeName, s.collectionName, docId,
					),
					Headers: map[string]string{
						"Authorization":        s.basicRestCreds,
						"X-CB-DurabilityLevel": "Majority",
					},
					Body: []byte(`abcde`),
				})
			},
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			resp := tc.fn()
			requireRestError(s.T(), resp, http.StatusBadRequest, &testRestError{
				Code: "DurabilityImpossible",
			})
		})
	}
}
