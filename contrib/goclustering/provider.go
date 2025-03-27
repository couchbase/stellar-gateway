/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package goclustering

import "context"

type Membership interface {
	UpdateMetaData(ctx context.Context, metaData []byte) error
	Leave(ctx context.Context) error
}

/*
Note that the Join/Leave calls must not be called concurrently.  It is however
safe to concurrently call Join or Leave alongside Watch/Get calls.
*/
type Provider interface {
	Join(ctx context.Context, memberID string, metaData []byte) (Membership, error)

	Watch(ctx context.Context) (chan *Snapshot, error)
	Get(ctx context.Context) (*Snapshot, error)
}
