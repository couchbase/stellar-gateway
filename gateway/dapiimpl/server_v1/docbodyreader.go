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
