package topology

import (
	"context"
	"encoding/json"
	"log"

	"github.com/couchbase/stellar-nebula/utils/etcdmemberlist"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type jsonEtcdNodeMetaData struct {
	NodeID        string `json:"node_id"`
	AdvertiseAddr string `json:"advertise_addr"`
	AdvertisePort int    `json:"advertise_port"`
	ServerGroup   string `json:"server_group"`
}

type EtcdProviderOptions struct {
	EtcdClient *clientv3.Client
	KeyPrefix  string
}

type EtcdProvider struct {
	etcdClient *clientv3.Client
	keyPrefix  string

	memberList *etcdmemberlist.MemberList
	membership *etcdmemberlist.Membership
}

func NewEtcdProvider(opts EtcdProviderOptions) (*EtcdProvider, error) {
	p := &EtcdProvider{
		etcdClient: opts.EtcdClient,
		keyPrefix:  opts.KeyPrefix,
	}

	err := p.init()
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (t *EtcdProvider) init() error {
	ml, err := etcdmemberlist.NewMemberList(etcdmemberlist.MemberListOptions{
		EtcdClient: t.etcdClient,
		KeyPrefix:  t.keyPrefix,
	})
	if err != nil {
		return err
	}

	t.memberList = ml

	return nil
}

func (tp *EtcdProvider) Join(localConfig *Endpoint) error {
	if tp.membership != nil {
		return ErrAlreadyJoined
	}

	metaData := jsonEtcdNodeMetaData{
		AdvertiseAddr: localConfig.AdvertiseAddr,
		AdvertisePort: localConfig.AdvertisePort,
		ServerGroup:   localConfig.ServerGroup,
	}

	metaDataBytes, err := json.Marshal(metaData)
	if err != nil {
		return err
	}

	mb, err := tp.memberList.Join(context.Background(), &etcdmemberlist.JoinOptions{
		MemberID: localConfig.NodeID,
		MetaData: metaDataBytes,
	})
	if err != nil {
		return err
	}

	tp.membership = mb

	return nil
}

func (tp *EtcdProvider) Leave() error {
	if tp.membership == nil {
		return ErrNotJoined
	}

	err := tp.membership.Leave(context.Background())
	if err != nil {
		return err
	}

	tp.membership = nil

	return nil
}

func (tp *EtcdProvider) procMemberEntry(entry *etcdmemberlist.Member) (*Endpoint, error) {
	var metaData jsonEtcdNodeMetaData
	err := json.Unmarshal(entry.MetaData, &metaData)
	if err != nil {
		return nil, err
	}

	return &Endpoint{
		NodeID:        entry.MemberID,
		AdvertiseAddr: metaData.AdvertiseAddr,
		AdvertisePort: metaData.AdvertisePort,
		ServerGroup:   metaData.ServerGroup,
	}, nil
}

func (tp *EtcdProvider) procMemberList(snap *etcdmemberlist.MembersSnapshot) (*Snapshot, error) {
	var members []Endpoint
	for _, entry := range snap.Members {
		member, err := tp.procMemberEntry(entry)
		if err != nil {
			return nil, err
		}

		members = append(members, *member)
	}

	return &Snapshot{
		Endpoints: members,
	}, nil
}

func (tp *EtcdProvider) Watch() (chan *Snapshot, error) {
	// TODO(brett19): Without context support, this is uncancellable...

	snapEvts, err := tp.memberList.WatchMembers(context.Background())
	if err != nil {
		return nil, err
	}

	outputCh := make(chan *Snapshot, 1)

	// we parse the first event to make sure there aren't an errors we need
	// to directly return...
	// TODO(brett19): Handle the channel closing before the first snapshot.
	firstSnap := <-snapEvts
	firstOutput, err := tp.procMemberList(firstSnap)
	if err != nil {
		return nil, err
	}

	// if everything went well, put the first snapshot into the channel
	outputCh <- firstOutput

	// start a goroutine to handle the remaining snapshots...
	go func() {
		for snap := range snapEvts {
			output, err := tp.procMemberList(snap)
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

func (tp *EtcdProvider) Get() (*Snapshot, error) {
	memberSnap, err := tp.memberList.Members(context.Background())
	if err != nil {
		return nil, err
	}

	snap, err := tp.procMemberList(memberSnap)
	if err != nil {
		return nil, err
	}

	return snap, nil
}
