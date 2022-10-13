package psclustering

import (
	"context"
	"encoding/json"

	"github.com/couchbase/stellar-nebula/common/clustering"
)

type Membership struct {
	ms clustering.Membership
}

func (m *Membership) UpdateMetaData(ctx context.Context, data *Member) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return m.ms.UpdateMetaData(ctx, dataBytes)
}

func (m *Membership) Leave(ctx context.Context) error {
	return m.ms.Leave(ctx)
}
