/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package server_v1

import (
	"errors"
	"io"
)

var (
	ErrSizeLimitExceeded = errors.New("size limit exceeded")
)

type sizeLimitReader struct {
	R io.Reader
	N int64
}

func (l *sizeLimitReader) Read(p []byte) (n int, err error) {
	if l.N <= 0 {
		return 0, ErrSizeLimitExceeded
	}
	if int64(len(p)) > l.N {
		p = p[0:l.N]
	}
	n, err = l.R.Read(p)
	l.N -= int64(n)
	return
}

func readDocFromHttpBody(r io.Reader) ([]byte, error) {
	return io.ReadAll(&sizeLimitReader{
		R: r,
		N: 20 * 1024 * 1024, // 20MB
	})
}
