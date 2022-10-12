package remotetopology

type Node struct {
	NodeID      string
	ServerGroup string
}

type DataNode struct {
	Node *Node

	LocalVbuckets []uint32
	GroupVbuckets []uint32
}

type Topology struct {
	RevEpoch uint64
	Revision uint64

	Nodes []*Node
}

type BucketTopology struct {
	RevEpoch uint64
	Revision uint64

	Nodes     []*Node
	DataNodes []*DataNode
}
