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
	Revision []uint64

	Nodes     []*Node
	DataNodes []*DataNode
}
