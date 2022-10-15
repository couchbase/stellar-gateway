package topology

import (
	"github.com/couchbase/stellar-nebula/common/nebclustering"
	"github.com/couchbase/stellar-nebula/common/remotetopology"
)

type Topology struct {
	Revision       []uint64
	LocalTopology  *nebclustering.Snapshot
	RemoteTopology *remotetopology.Topology
}
