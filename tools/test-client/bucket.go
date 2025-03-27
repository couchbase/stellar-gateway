/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package gocbps

type Bucket struct {
	client     *Client
	bucketName string
}

func (b *Bucket) Scope(scopeName string) *Scope {
	return &Scope{
		bucket:    b,
		scopeName: scopeName,
	}
}

func (b *Bucket) Collection(collectionName string) *Collection {
	return b.Scope("_default").Collection(collectionName)
}

func (b *Bucket) DefaultCollection() *Collection {
	return b.Scope("_default").Collection("_default")
}
