package clustering

import "errors"

var (
	ErrAlreadyJoined = errors.New("already joined")
	ErrNotJoined     = errors.New("not joined")
)