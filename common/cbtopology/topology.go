package cbtopology

type Node struct {
	HostID      string
	NodeID      string
	ServerGroup string

	HasMgmt      bool
	HasKv        bool
	HasViews     bool
	HasQuery     bool
	HasAnalytics bool
	HasSearch    bool
}

type DataNode struct {
	Node *Node

	Vbuckets        []int
	VbucketReplicas []int
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
