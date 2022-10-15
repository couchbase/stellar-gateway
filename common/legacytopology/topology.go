package legacytopology

type Node struct {
	NodeID      string
	ServerGroup string
	Address     string
}

type DataNode struct {
	Node *Node
}

type VbucketRouting struct {
	Nodes    []*DataNode
	Vbuckets []uint32
}

type Topology struct {
	Revision []uint64

	Nodes          []*Node
	VbucketRouting *VbucketRouting
}
