/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package clustering

// The JSON representation of this data is intentionally terse in order to allow
// it to potentially fit easily in UDP gossip messages.

type ServicePorts struct {
	Mgmt      int `json:"m,omitempty"`
	KV        int `json:"k,omitempty"`
	Views     int `json:"v,omitempty"`
	Query     int `json:"q,omitempty"`
	Search    int `json:"s,omitempty"`
	Analytics int `json:"a,omitempty"`

	MgmtTls      int `json:"mt,omitempty"`
	KVTls        int `json:"kt,omitempty"`
	ViewsTls     int `json:"vt,omitempty"`
	QueryTls     int `json:"qt,omitempty"`
	SearchTls    int `json:"st,omitempty"`
	AnalyticsTls int `json:"at,omitempty"`
}

type Member struct {
	MemberID       string       `json:"-"`
	ServerGroup    string       `json:"sg,omitempty"`
	AdvertiseAddr  string       `json:"aa,omitempty"`
	AdvertisePorts ServicePorts `json:"ap,omitempty"`
}

type Snapshot struct {
	Revision []uint64
	Members  []*Member
}
