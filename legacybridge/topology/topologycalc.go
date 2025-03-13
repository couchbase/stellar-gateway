/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package topology

import (
	"github.com/couchbase/stellar-gateway/client"
	"github.com/couchbase/stellar-gateway/contrib/revisionarr"
	"github.com/couchbase/stellar-gateway/legacybridge/clustering"
)

func ComputeTopology(
	lt *clustering.Snapshot,
	rt *client.Topology,
) (*Topology, error) {
	var nodes []*Node

	// build the nodes lists first
	for _, lclNode := range lt.Members {
		node := &Node{
			NodeID:      lclNode.MemberID,
			ServerGroup: lclNode.ServerGroup,
		}
		nodes = append(nodes, node)
	}

	var vbucketRouting *VbucketRouting
	if rt.VbucketRouting != nil {
		var dataNodes []*DataNode

		// build the list of data nodes
		for _, node := range nodes {
			dataNode := &DataNode{
				Node: node,
			}
			dataNodes = append(dataNodes, dataNode)
		}

		// TODO(brett19): Optimally assign vbuckets to servers.
		// I believe this is actually a harder problem than it seems at first glance and may
		// actually require an iterative approach... Basically it might be an optimization problem.
		// For now we just assign them linearly...
		vbucketAssignment := make([]uint32, rt.VbucketRouting.NumVbuckets)
		numDataNodes := uint(len(dataNodes))
		for vbId := uint(0); vbId < rt.VbucketRouting.NumVbuckets; vbId++ {
			vbucketAssignment[vbId] = uint32(vbId % numDataNodes)
		}

		vbucketRouting = &VbucketRouting{
			Nodes:    dataNodes,
			Vbuckets: vbucketAssignment,
		}
	}

	// TODO(brett19): Need properly stored revision numbers from ETCD.
	// This is not actually safe for legacy clients, but it'll do for now...
	mergeRevision := revisionarr.Add(lt.Revision, rt.Revision)

	return &Topology{
		Revision:       mergeRevision,
		Nodes:          nodes,
		VbucketRouting: vbucketRouting,
	}, nil
}
