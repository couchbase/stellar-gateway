/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package goclustering

import (
	"context"
	"sync"

	"golang.org/x/exp/slices"
)

type InProcProviderOptions struct {
	DisableVersions bool
}

type inProcMembership struct {
	parent   *InProcProvider
	memberID string
	metaData []byte
}

type InProcProvider struct {
	lock     sync.Mutex
	revision uint64
	members  []*inProcMembership
	watchers []chan *Snapshot
}

var _ Provider = (*InProcProvider)(nil)

func NewInProcProvider(opts InProcProviderOptions) (*InProcProvider, error) {
	var initialVersion uint64 = 1
	if opts.DisableVersions {
		initialVersion = 0
	}

	return &InProcProvider{
		revision: initialVersion,
	}, nil
}

func (p *InProcProvider) getSnapLocked() *Snapshot {
	revision := []uint64{p.revision}
	var members []*Member
	for _, memberI := range p.members {
		members = append(members, &Member{
			MemberID: memberI.memberID,
			MetaData: memberI.metaData,
		})
	}

	return &Snapshot{
		Revision: revision,
		Members:  members,
	}
}

func (p *InProcProvider) signalUpdatedLocked() {
	if p.revision > 0 {
		p.revision++
	}

	newSnap := p.getSnapLocked()

	for _, outputCh := range p.watchers {
		outputCh <- newSnap
	}
}

func (p *InProcProvider) addMemberLocked(m *inProcMembership) {
	p.members = append(p.members, m)
}

func (p *InProcProvider) removeMemberLocked(m *inProcMembership) bool {
	memberIdx := slices.Index(p.members, m)
	if memberIdx == -1 {
		return false
	}

	membersLen := len(p.members)
	p.members[memberIdx] = p.members[membersLen-1]
	p.members = p.members[:membersLen-1]

	return true
}

func (p *InProcProvider) addWatcherLocked(ch chan *Snapshot) {
	p.watchers = append(p.watchers, ch)
}

func (p *InProcProvider) removeWatcherLocked(ch chan *Snapshot) bool {
	watcherIdx := slices.Index(p.watchers, ch)
	if watcherIdx == -1 {
		return false
	}

	slices.Delete(p.watchers, watcherIdx, watcherIdx+1)

	return true
}

func (p *InProcProvider) Join(ctx context.Context, memberID string, metaData []byte) (Membership, error) {
	metaData = slices.Clone(metaData)

	p.lock.Lock()
	m := &inProcMembership{
		parent:   p,
		memberID: memberID,
		metaData: metaData,
	}
	p.addMemberLocked(m)
	p.signalUpdatedLocked()

	p.lock.Unlock()

	return m, nil
}

func (m *inProcMembership) UpdateMetaData(ctx context.Context, metaData []byte) error {
	metaData = slices.Clone(metaData)

	m.parent.lock.Lock()

	m.metaData = metaData
	m.parent.signalUpdatedLocked()

	m.parent.lock.Unlock()

	return nil
}

func (m *inProcMembership) Leave(ctx context.Context) error {
	m.parent.lock.Lock()

	if !m.parent.removeMemberLocked(m) {
		m.parent.lock.Unlock()
		return ErrAlreadyLeft
	}

	m.parent.signalUpdatedLocked()

	m.parent.lock.Unlock()

	return nil
}

func (p *InProcProvider) Watch(ctx context.Context) (chan *Snapshot, error) {
	// we buffer this channel by one to make it easier to avoid blocking inside
	// of the critical

	signalCh := make(chan *Snapshot)

	p.lock.Lock()

	currentSnap := p.getSnapLocked()
	p.addWatcherLocked(signalCh)

	p.lock.Unlock()

	// in order to guarentee the ordering of the snapshots that are delivered
	// to the output, we need to run this goroutine which enforces it.
	outputCh := make(chan *Snapshot)
	go func() {
		outputCh <- currentSnap
		for newSnap := range signalCh {
			outputCh <- newSnap
		}
		close(outputCh)
	}()

	// Watch for the context being cancelled and shut down the watcher when
	// that happens.  The outputCh is closed by the goroutine above in response
	// to the signal channel being closed.
	go func() {
		<-ctx.Done()

		p.lock.Lock()
		p.removeWatcherLocked(signalCh)
		p.lock.Unlock()

		close(signalCh)
	}()

	return outputCh, nil
}

func (p *InProcProvider) Get(ctx context.Context) (*Snapshot, error) {
	p.lock.Lock()

	snap := p.getSnapLocked()

	p.lock.Unlock()

	return snap, nil
}
