package gocbps

import (
	"github.com/couchbase/stellar-nebula/genproto/kv_v1"
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
