package gocbps

import couchbase_v1 "github.com/couchbase/stellar-nebula/genproto/v1"

type Cas uint64

func (cas Cas) toProto() *couchbase_v1.Cas {
	if cas == 0 {
		return nil
	}

	protosCas := &couchbase_v1.Cas{
		Value: uint64(cas),
	}
	return protosCas
}
