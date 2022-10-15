package topologycalc

type OutputNode struct {
	NodeID      string
	ServerGroup string
}

type OutputDataNode struct {
	Node *OutputNode

	LocalVbuckets []uint32
	GroupVbuckets []uint32
}

type OutputTopology struct {
	Revision []uint64

	Nodes     []*OutputNode
	DataNodes []*OutputDataNode
}
