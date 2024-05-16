package system

import (
	"fmt"
	"net"
)

type ListenersOptions struct {
	Address  string
	DataPort int
	SdPort   int
	DapiPort int
}

type Listeners struct {
	dataListener net.Listener
	sdListener   net.Listener
	dapiListener net.Listener
}

func NewListeners(opts *ListenersOptions) (*Listeners, error) {
	var err error
	l := &Listeners{}

	if opts.DataPort >= 0 {
		l.dataListener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", opts.Address, opts.DataPort))
		if err != nil {
			l.Close()
			return nil, err
		}
	}

	if opts.SdPort >= 0 {
		l.sdListener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", opts.Address, opts.SdPort))
		if err != nil {
			l.Close()
			return nil, err
		}
	}

	if opts.DapiPort >= 0 {
		l.dapiListener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", opts.Address, opts.DapiPort))
		if err != nil {
			l.Close()
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

func (l *Listeners) BoundSdPort() int {
	if l.sdListener == nil {
		return 0
	}
	return l.sdListener.Addr().(*net.TCPAddr).Port
}

func (l *Listeners) BoundDapiPort() int {
	if l.dapiListener == nil {
		return 0
	}
	return l.dapiListener.Addr().(*net.TCPAddr).Port
}

func (l *Listeners) Close() error {
	if l.dataListener != nil {
		l.dataListener.Close()
		l.dataListener = nil
	}
	if l.sdListener != nil {
		l.sdListener.Close()
		l.sdListener = nil
	}
	if l.dapiListener != nil {
		l.dapiListener.Close()
		l.dapiListener = nil
	}

	return nil
}
