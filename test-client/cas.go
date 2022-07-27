package gocbps

import "github.com/couchbase/stellar-nebula/protos"

type Cas uint64

func (cas Cas) toProto() *protos.Cas {
	if cas == 0 {
		return nil
	}

	protosCas := &protos.Cas{
		Value: uint64(cas),
	}
	return protosCas
}
