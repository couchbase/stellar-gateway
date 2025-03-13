/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

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

type VbucketMapping struct {
	Nodes       []*DataNode
	NumVbuckets uint
}

type Topology struct {
	RevEpoch uint64
	Revision uint64

	Nodes          []*Node
	VbucketMapping *VbucketMapping
}
