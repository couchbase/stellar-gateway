package clustering

import (
	"context"
	"encoding/json"

	"github.com/couchbase/stellar-gateway/contrib/goclustering"
	"go.uber.org/zap"
)

type Membership struct {
	ms goclustering.Membership
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

type Manager struct {
	Provider goclustering.Provider
	Logger *zap.Logger
}

var _ Provider = (*Manager)(nil)

func (m *Manager) Join(ctx context.Context, data *Member) (*Membership, error) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	ms, err := m.Provider.Join(ctx, data.MemberID, dataBytes)
	if err != nil {
		return nil, err
	}

	return &Membership{ms}, nil
}

func (p *Manager) procSnapshot(snap *goclustering.Snapshot) *Snapshot {
	var members []*Member
	for _, entry := range snap.Members {
		var member Member
		err := json.Unmarshal(entry.MetaData, &member)
		if err != nil {
			// we intentionally don't bail here so that members with bad meta-data
			// still appear in the snapshot, but just are missing all their data.
			p.Logger.Error("failed to unmarshal member", zap.Error(err))
		}

		member.MemberID = entry.MemberID
		members = append(members, &member)
	}

	return &Snapshot{
		Revision: snap.Revision,
		Members:  members,
	}
}

func (m *Manager) Watch(ctx context.Context) (chan *Snapshot, error) {
	outputCh := make(chan *Snapshot)

	// start watching the underlying provider for updates
	snapCh, err := m.Provider.Watch(ctx)
	if err != nil {
		return nil, err
	}

	// start a goroutine to perform translation to our format.
	go func() {
		for pSnap := range snapCh {
			outputCh <- m.procSnapshot(pSnap)
		}
		close(outputCh)
	}()

	return outputCh, nil
}

func (m *Manager) Get(ctx context.Context) (*Snapshot, error) {
	pSnap, err := m.Provider.Get(ctx)
	if err != nil {
		return nil, err
	}

	snap := m.procSnapshot(pSnap)
	return snap, nil
}
