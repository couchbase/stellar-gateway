package pstopology

type Node struct {
	NodeID      string
	ServerGroup string
	Address     string
}

type DataNode struct {
	Node *Node

	LocalVbuckets []uint32
	GroupVbuckets []uint32
}

type VbucketDataRouting struct {
	NumVbuckets uint32
	DataNodes   []*DataNode
}

type Topology struct {
	RevEpoch uint64
	Revision uint64

	Nodes              []*Node
	VbucketDataRouting *VbucketDataRouting
}
