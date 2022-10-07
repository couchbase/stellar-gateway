package etcdmemberlist

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"
)

type MemberListOptions struct {
	EtcdClient *etcd.Client
	KeyPrefix  string
}

type MemberList struct {
	etcdClient *etcd.Client
	keyPrefix  string
}

type Member struct {
	MemberID string
	MetaData []byte
}

type MembersSnapshot struct {
	Revision int64
	Members  []*Member
}

func NewMemberList(opts MemberListOptions) (*MemberList, error) {
	return &MemberList{
		etcdClient: opts.EtcdClient,
		keyPrefix:  opts.KeyPrefix,
	}, nil
}

type JoinOptions struct {
	MemberID    string
	MetaData    []byte
	LeasePeriod time.Duration
}

func (ml *MemberList) Join(ctx context.Context, opts *JoinOptions) (*Membership, error) {
	if opts == nil {
		opts = &JoinOptions{}
	}

	memberID := ""
	if opts.MemberID != "" {
		memberID = opts.MemberID
	} else {
		memberID = uuid.NewString()
	}

	leasePeriod := 5 * time.Second
	if opts.LeasePeriod != 0 {
		// minimum lease period is 5 seconds...  etcdv3 also has this restriction
		if opts.LeasePeriod < 5*time.Second {
			return nil, errors.New("lease period must be at least 5 seconds")
		}

		leasePeriod = opts.LeasePeriod
	}

	m := &Membership{
		etcdClient:  ml.etcdClient,
		keyPrefix:   ml.keyPrefix,
		leasePeriod: leasePeriod,
		id:          memberID,
		metaData:    opts.MetaData,
	}

	err := m.join(ctx)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (ml *MemberList) Members(ctx context.Context) (*MembersSnapshot, error) {
	membersPrefix := ml.keyPrefix + "/"
	resp, err := ml.etcdClient.KV.Get(ctx, membersPrefix, etcd.WithPrefix())
	if err != nil {
		return nil, err
	}

	var members []*Member
	for _, memberResp := range resp.Kvs {
		memberID := memberResp.Key[len(membersPrefix):]

		members = append(members, &Member{
			MemberID: string(memberID),
			MetaData: memberResp.Value,
		})
	}

	return &MembersSnapshot{
		Revision: resp.Header.Revision,
		Members:  members,
	}, nil
}

func (ml *MemberList) WatchMembers(ctx context.Context) (chan *MembersSnapshot, error) {
	// TODO(brett19): We should coalesce multiple watch calls.
	// This would avoid unneccessary client-server traffic.  It can probably be implemented
	// by always having a watch running and then just join these Watch calls to it.

	outputCh := make(chan *MembersSnapshot, 1)
	keyMapRevision := int64(0)
	keyMap := make(map[string][]byte)

	membersPrefix := ml.keyPrefix + "/"

	emitKeyMap := func() {
		var members []*Member
		for memberKey, memberData := range keyMap {
			memberID := memberKey[len(membersPrefix):]

			members = append(members, &Member{
				MemberID: string(memberID),
				MetaData: memberData,
			})
		}

		outputCh <- &MembersSnapshot{
			Revision: keyMapRevision,
			Members:  members,
		}
	}

	// fetch the initial state of the members
	resp, err := ml.etcdClient.KV.Get(ctx, membersPrefix, etcd.WithPrefix())
	if err != nil {
		return nil, err
	}

	// Populate the initial key-map from the get request
	for _, memberResp := range resp.Kvs {
		keyMap[string(memberResp.Key)] = memberResp.Value
	}
	keyMapRevision = resp.Header.Revision

	// Emit the initial members list
	emitKeyMap()

	watchCh := ml.etcdClient.Watcher.Watch(ctx, membersPrefix, etcd.WithRev(resp.Header.Revision))
	go func() {
		for {
			watchEvts, ok := <-watchCh
			if !ok {
				close(outputCh)
				break
			}

			// update our key-map with the events
			for _, watchEvt := range watchEvts.Events {
				switch watchEvt.Type {
				case mvccpb.PUT:
					keyMap[string(watchEvt.Kv.Key)] = watchEvt.Kv.Value
				case mvccpb.DELETE:
					delete(keyMap, string(watchEvt.Kv.Key))
				default:
					// TODO(brett19): Handle unexpected event types...
				}
			}
			keyMapRevision = watchEvts.Header.Revision

			// emit the updated events
			emitKeyMap()
		}
	}()

	return outputCh, nil
}
