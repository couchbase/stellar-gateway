package pstopology

type Node struct {
	NodeID      string
	ServerGroup string
	Address     string
	Port        int
}

type DataNode struct {
	Node *Node

	LocalVbuckets []uint32
	GroupVbuckets []uint32
}

type VbucketRouting struct {
	NumVbuckets uint
	Nodes       []*DataNode
}

type Topology struct {
	Revision []uint64

	Nodes          []*Node
	VbucketRouting *VbucketRouting
}
