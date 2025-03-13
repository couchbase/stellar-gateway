/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package authhdr_test

import (
	"net/http"
	"testing"

	"github.com/couchbase/stellar-gateway/utils/authhdr"
)

var TEST_HEADER string = "Basic YWxhZGRpbjpvcGVuc2VzYW1l"

func TestBasic(t *testing.T) {
	r := http.Request{
		Header: map[string][]string{
			"Authorization": {TEST_HEADER},
		},
	}
	httpUser, httpPass, ok := r.BasicAuth()
	if !ok {
		t.Fatalf("Failed to http decode header")
	}

	username, password, ok := authhdr.DecodeBasicAuth(TEST_HEADER)
	if !ok {
		t.Fatalf("Failed to decode header")
	}
	if username != httpUser {
		t.Fatalf("Username mismatch: %s", username)
	}
	if password != httpPass {
		t.Fatalf("Password mismatch: %s", password)
	}
}

func BenchmarkHttp(b *testing.B) {
	for i := 0; i < b.N; i++ {
		r := http.Request{
			Header: map[string][]string{
				"Authorization": {TEST_HEADER},
			},
		}
		_, _, ok := r.BasicAuth()
		if !ok {
			b.Fatalf("Failed to decode header")
		}
	}
}

func BenchmarkLib(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _, ok := authhdr.DecodeBasicAuth(TEST_HEADER)
		if !ok {
			b.Fatalf("Failed to decode header")
		}
	}
}
