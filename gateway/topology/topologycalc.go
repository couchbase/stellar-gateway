package topology

import (
	"github.com/couchbase/stellar-nebula/contrib/cbtopology"
	"github.com/couchbase/stellar-nebula/contrib/revisionarr"
	"github.com/couchbase/stellar-nebula/gateway/clustering"
	"github.com/couchbase/stellar-nebula/utils/sliceutils"
	"golang.org/x/exp/slices"
)

func ComputeTopology(
	lt *clustering.Snapshot,
	rt *cbtopology.Topology,
) (*Topology, error) {
	var nodes []*Node

	// build the nodes lists first
	for _, lclNode := range lt.Members {
		// all nodes who did not disable PS are considered part of the topology
		node := &Node{
			NodeID:      lclNode.MemberID,
			ServerGroup: lclNode.ServerGroup,

			Address: lclNode.AdvertiseAddr,
			Port:    lclNode.AdvertisePorts.PS,
		}
		nodes = append(nodes, node)
	}

	var vbucketRouting *VbucketRouting
	if rt.VbucketMapping != nil {
		var dataNodes []*DataNode
		for _, node := range nodes {
			// all local nodes are considered capable of processing data...
			dataNode := &DataNode{
				Node: node,
			}

			for _, rmtDataNode := range rt.VbucketMapping.Nodes {
				// if we have the same NodeID as one of the remote nodes, we take
				// all their local vbuckets and make them our own.
				if rmtDataNode.Node.NodeID == node.NodeID {
					for vbId := range rmtDataNode.Vbuckets {
						dataNode.LocalVbuckets = append(dataNode.LocalVbuckets, uint32(vbId))
					}
				}

				// if we are in the same group as another node, their local and group
				// vbuckets become our group vbuckets.
				if rmtDataNode.Node.ServerGroup == node.ServerGroup {
					for vbId := range rmtDataNode.Vbuckets {
						dataNode.LocalVbuckets = append(dataNode.LocalVbuckets, uint32(vbId))
					}
				}
			}

			// deduplicate the vbucket lists
			dataNode.LocalVbuckets = sliceutils.RemoveDuplicates(dataNode.LocalVbuckets)
			dataNode.GroupVbuckets = sliceutils.RemoveDuplicates(dataNode.GroupVbuckets)

			// sort the vbucket lists
			slices.Sort(dataNode.LocalVbuckets)
			slices.Sort(dataNode.GroupVbuckets)

			dataNodes = append(dataNodes, dataNode)
		}

		vbucketRouting = &VbucketRouting{
			NumVbuckets: rt.VbucketMapping.NumVbuckets,
			Nodes:       dataNodes,
		}
	}

	// Due to the nature of Protostellar not permitting clients to consider multiple configuration
	// streams at the same time, we are safe to consider an revision addition sufficient.
	mergeRevision := revisionarr.Add(lt.Revision, []uint64{rt.Revision, rt.RevEpoch})

	return &Topology{
		Revision:       mergeRevision,
		Nodes:          nodes,
		VbucketRouting: vbucketRouting,
	}, nil
}
