/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package client

type Node struct {
	NodeID      string
	ServerGroup string
}

type DataNode struct {
	Node *Node

	LocalVbuckets []uint32
	GroupVbuckets []uint32
}

type VbucketRouting struct {
	Nodes       []*DataNode
	NumVbuckets uint
}

type Topology struct {
	Revision []uint64

	Nodes          []*Node
	VbucketRouting *VbucketRouting
}
