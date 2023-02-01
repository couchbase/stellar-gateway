package goclustering

import (
	"context"
	"log"

	"github.com/couchbase/stellar-gateway/contrib/etcdmemberlist"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdProviderOptions struct {
	EtcdClient *clientv3.Client
	KeyPrefix  string
}

type EtcdProvider struct {
	ml *etcdmemberlist.MemberList
}

var _ Provider = (*EtcdProvider)(nil)

func NewEtcdProvider(opts EtcdProviderOptions) (*EtcdProvider, error) {
	ml, err := etcdmemberlist.NewMemberList(etcdmemberlist.MemberListOptions{
		EtcdClient: opts.EtcdClient,
		KeyPrefix:  opts.KeyPrefix,
	})
	if err != nil {
		return nil, err
	}

	return &EtcdProvider{
		ml: ml,
	}, nil
}

func (p *EtcdProvider) Join(ctx context.Context, memberID string, metaData []byte) (Membership, error) {
	mb, err := p.ml.Join(ctx, &etcdmemberlist.JoinOptions{
		MemberID: memberID,
		MetaData: metaData,
	})
	if err != nil {
		return nil, err
	}

	return &etcdMembership{mb}, nil
}

type etcdMembership struct {
	ms *etcdmemberlist.Membership
}

func (m *etcdMembership) UpdateMetaData(ctx context.Context, metaData []byte) error {
	return m.ms.SetMetaData(ctx, metaData)
}

func (m *etcdMembership) Leave(ctx context.Context) error {
	return m.ms.Leave(ctx)
}

func (p *EtcdProvider) procMemberList(snap *etcdmemberlist.MembersSnapshot) (*Snapshot, error) {
	var members []*Member
	for _, entry := range snap.Members {
		members = append(members, &Member{
			MemberID: entry.MemberID,
			MetaData: entry.MetaData,
		})
	}

	return &Snapshot{
		Revision: []uint64{uint64(snap.Revision)},
		Members:  members,
	}, nil
}

func (p *EtcdProvider) Watch(ctx context.Context) (chan *Snapshot, error) {
	snapEvts, err := p.ml.WatchMembers(ctx)
	if err != nil {
		return nil, err
	}

	outputCh := make(chan *Snapshot, 1)

	// we parse the first event to make sure there aren't an errors we need
	// to directly return...
	// TODO(brett19): Handle the channel closing before the first snapshot.
	firstSnap := <-snapEvts
	firstOutput, err := p.procMemberList(firstSnap)
	if err != nil {
		return nil, err
	}

	// if everything went well, put the first snapshot into the channel
	outputCh <- firstOutput

	// start a goroutine to handle the remaining snapshots...
	go func() {
		for snap := range snapEvts {
			output, err := p.procMemberList(snap)
			if err != nil {
				// if an error occurs, we can't directly return it to the user, so instead we simply
				// break out of the read loop and close the output channel.  If the user attempts to
				// restart the watch, they should get the proper error returned.
				// TODO(brett19): Need to close the input watcher channel somehow...
				log.Printf("etcd topology provider failed to parse a config from the memberlist: %s", err)
				break
			}

			outputCh <- output
		}

		close(outputCh)
	}()

	return outputCh, nil
}

func (p *EtcdProvider) Get(ctx context.Context) (*Snapshot, error) {
	memberSnap, err := p.ml.Members(ctx)
	if err != nil {
		return nil, err
	}

	snap, err := p.procMemberList(memberSnap)
	if err != nil {
		return nil, err
	}

	return snap, nil
}
