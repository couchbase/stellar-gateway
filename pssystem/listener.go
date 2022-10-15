package pssystem

import (
	"fmt"
	"net"
)

type ListenerOptions struct {
	Address string
	Port    int
}

type Listener struct {
	l net.Listener
}

func NewListener(opts *ListenerOptions) (*Listener, error) {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", opts.Address, opts.Port))
	if err != nil {
		return nil, err
	}

	return &Listener{l: l}, nil
}

func (l *Listener) BoundPort() int {
	if l.l == nil {
		return 0
	}
	return l.l.Addr().(*net.TCPAddr).Port
}

func (l *Listener) Close() error {
	return l.l.Close()
}
