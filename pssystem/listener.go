package pssystem

import (
	"fmt"
	"net"
)

type ListenerOptions struct {
	Address string
	Port    int
}

func NewListener(opts *ListenerOptions) (net.Listener, error) {
	return net.Listen("tcp", fmt.Sprintf("%s:%d", opts.Address, opts.Port))
}
