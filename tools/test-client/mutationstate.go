/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package gocbps

import (
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
)

type MutationToken struct {
	VbID       uint16
	VbUUID     uint64
	SeqNo      uint64
	BucketName string
}

type MutationState struct {
	Tokens []MutationToken
}

func mutationTokenFromPs(token *kv_v1.MutationToken) *MutationToken {
	if token == nil {
		return nil
	}

	return &MutationToken{
		VbID:       uint16(token.VbucketId),
		VbUUID:     token.VbucketUuid,
		SeqNo:      token.SeqNo,
		BucketName: token.BucketName,
	}
}
