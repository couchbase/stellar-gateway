package topologycalc

type LocalNode struct {
	NodeID      string
	ServerGroup string
}

type LocalTopology struct {
	Revision []uint64
	Nodes    []*LocalNode
}
