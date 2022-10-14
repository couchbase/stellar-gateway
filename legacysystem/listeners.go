package legacysystem

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
)

type ServicePorts struct {
	Mgmt      int
	KV        int
	Query     int
	Search    int
	Analytics int
}

func (p ServicePorts) isAllDisabled() bool {
	hasValue := false
	hasValue = hasValue || (p.Mgmt >= 0)
	hasValue = hasValue || (p.KV >= 0)
	hasValue = hasValue || (p.Query >= 0)
	hasValue = hasValue || (p.Search >= 0)
	hasValue = hasValue || (p.Analytics >= 0)
	return hasValue
}

type ListenersOptions struct {
	Address   string
	Ports     ServicePorts
	TLSPorts  ServicePorts
	TLSConfig *tls.Config
}

type Listeners struct {
	mgmtListener  net.Listener
	kvListener    net.Listener
	queryListener net.Listener

	mgmtTLSListener  net.Listener
	kvTLSListener    net.Listener
	queryTLSListener net.Listener
}

func NewListeners(opts *ListenersOptions) (*Listeners, error) {
	if !opts.TLSPorts.isAllDisabled() && opts.TLSConfig == nil {
		return nil, errors.New("must specify TLS config when TLS ports are used")
	}

	makePlain := func(port int) (net.Listener, error) {
		if port == -1 {
			return nil, nil
		}

		return net.Listen("tcp", fmt.Sprintf("%s:%d", opts.Address, port))
	}

	makeTLS := func(port int) (net.Listener, error) {
		if port == -1 {
			return nil, nil
		}

		return tls.Listen(
			"tcp",
			fmt.Sprintf("%s:%d", opts.Address, port),
			opts.TLSConfig)
	}

	var err error
	l := &Listeners{}

	l.mgmtListener, err = makePlain(opts.Ports.Mgmt)
	if err != nil {
		l.Close()
		return nil, err
	}

	l.kvListener, err = makePlain(opts.Ports.KV)
	if err != nil {
		l.Close()
		return nil, err
	}

	l.queryListener, err = makePlain(opts.Ports.Query)
	if err != nil {
		l.Close()
		return nil, err
	}

	// TODO(brett19): Ensure TLS listeners support HTTP2 and what not.

	l.mgmtTLSListener, err = makeTLS(opts.TLSPorts.Mgmt)
	if err != nil {
		l.Close()
		return nil, err
	}

	l.kvTLSListener, err = makeTLS(opts.TLSPorts.KV)
	if err != nil {
		l.Close()
		return nil, err
	}

	l.queryTLSListener, err = makeTLS(opts.TLSPorts.Query)
	if err != nil {
		l.Close()
		return nil, err
	}

	return l, nil
}

func (l *Listeners) Close() error {
	if l.mgmtListener != nil {
		l.mgmtListener.Close()
		l.mgmtListener = nil
	}
	if l.kvListener != nil {
		l.kvListener.Close()
		l.kvListener = nil
	}
	if l.queryListener != nil {
		l.queryListener.Close()
		l.queryListener = nil
	}

	if l.mgmtTLSListener != nil {
		l.mgmtTLSListener.Close()
		l.mgmtTLSListener = nil
	}
	if l.kvTLSListener != nil {
		l.kvTLSListener.Close()
		l.kvTLSListener = nil
	}
	if l.queryTLSListener != nil {
		l.queryTLSListener.Close()
		l.queryTLSListener = nil
	}

	return nil
}
