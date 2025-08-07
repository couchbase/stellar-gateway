package system

import (
	"fmt"
	"net"
)

type ListenersOptions struct {
	Address  string
	DataPort int
	DapiPort int
}

type Listeners struct {
	dataListener net.Listener
	dapiListener net.Listener
}

func NewListeners(opts *ListenersOptions) (*Listeners, error) {
	var err error
	l := &Listeners{}

	if opts.DataPort >= 0 {
		l.dataListener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", opts.Address, opts.DataPort))
		if err != nil {
			_ = l.Close()
			return nil, err
		}
	}

	if opts.DapiPort >= 0 {
		l.dapiListener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", opts.Address, opts.DapiPort))
		if err != nil {
			_ = l.Close()
			return nil, err
		}
	}

	return l, nil
}

func (l *Listeners) BoundDataPort() int {
	if l.dataListener == nil {
		return 0
	}
	return l.dataListener.Addr().(*net.TCPAddr).Port
}

func (l *Listeners) BoundDapiPort() int {
	if l.dapiListener == nil {
		return 0
	}
	return l.dapiListener.Addr().(*net.TCPAddr).Port
}

func (l *Listeners) Close() error {
	if l.dataListener != nil {
		_ = l.dataListener.Close()
		l.dataListener = nil
	}
	if l.dapiListener != nil {
		_ = l.dapiListener.Close()
		l.dapiListener = nil
	}

	return nil
}
