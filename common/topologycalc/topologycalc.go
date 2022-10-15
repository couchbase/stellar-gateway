package topologycalc

import (
	"github.com/couchbase/stellar-nebula/common/remotetopology"
	"github.com/couchbase/stellar-nebula/contrib/revisionarr"
	"github.com/couchbase/stellar-nebula/utils/sliceutils"
	"golang.org/x/exp/slices"
)

func CalcTopology(lt *LocalTopology, rt *remotetopology.Topology) (*OutputTopology, error) {
	var nodes []*OutputNode
	var dataNodes []*OutputDataNode

	// build the nodes lists first
	for _, lclNode := range lt.Nodes {
		// all local nodes are considered part of the topology
		node := &OutputNode{
			NodeID:      lclNode.NodeID,
			ServerGroup: lclNode.ServerGroup,
		}

		// all local nodes are considered capable of processing data...
		dataNode := &OutputDataNode{
			Node: node,
		}

		for _, rmtDataNode := range rt.DataNodes {
			// if we have the same NodeID as one of the remote nodes, we take
			// all their local vbuckets and make them our own.
			if rmtDataNode.Node.NodeID == node.NodeID {
				dataNode.LocalVbuckets = append(dataNode.LocalVbuckets, rmtDataNode.LocalVbuckets...)
			}

			// if we are in the same group as another node, their local and group
			// vbuckets become our group vbuckets.
			if rmtDataNode.Node.ServerGroup == node.ServerGroup {
				dataNode.GroupVbuckets = append(dataNode.GroupVbuckets, rmtDataNode.LocalVbuckets...)
				dataNode.GroupVbuckets = append(dataNode.GroupVbuckets, rmtDataNode.GroupVbuckets...)
			}
		}

		// deduplicate the vbucket lists
		dataNode.LocalVbuckets = sliceutils.RemoveDuplicates(dataNode.LocalVbuckets)
		dataNode.GroupVbuckets = sliceutils.RemoveDuplicates(dataNode.GroupVbuckets)

		// sort the vbucket lists
		slices.Sort(dataNode.LocalVbuckets)
		slices.Sort(dataNode.GroupVbuckets)

		// add the nodes to the appropriate lists
		nodes = append(nodes, node)
		dataNodes = append(dataNodes, dataNode)
	}

	// The local topology and remote topology revisions must be monotonically increasing which
	// means that we can safely calculate the derived revision through addition.
	outRevision := revisionarr.Add(lt.Revision, rt.Revision)

	return &OutputTopology{
		Revision:  outRevision,
		Nodes:     nodes,
		DataNodes: dataNodes,
	}, nil
}
