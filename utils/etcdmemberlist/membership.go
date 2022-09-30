package etcdmemberlist

import (
	"context"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
)

type Membership struct {
	etcdClient  *etcd.Client
	keyPrefix   string
	leasePeriod time.Duration
	id          string
	metaData    []byte

	leaseID etcd.LeaseID
}

func (m *Membership) key() string {
	return m.keyPrefix + "/" + m.id
}

func (m *Membership) join(ctx context.Context) error {
	leaseTimeoutInSecs := int64(m.leasePeriod / time.Second)

	lease, err := m.etcdClient.Lease.Grant(ctx, int64(leaseTimeoutInSecs))
	if err != nil {
		return err
	}

	leaseID := lease.ID

	leaseKaCh, err := m.etcdClient.Lease.KeepAlive(context.Background(), leaseID)
	if err != nil {
		return err
	}

	go func() {
		// wait for the lease keep-alive to close
		for range leaseKaCh {
		}

		// TODO(brett19): Handle no longer holding our lease...
	}()

	m.leaseID = leaseID

	_, err = m.etcdClient.KV.Put(ctx, m.key(), string(m.metaData), etcd.WithLease(leaseID))
	if err != nil {
		return err
	}

	return nil
}

func (m *Membership) SetMetaData(ctx context.Context, data []byte) error {
	m.metaData = data

	_, err := m.etcdClient.KV.Put(ctx, m.key(), string(m.metaData), etcd.WithLease(m.leaseID))
	if err != nil {
		return err
	}

	return nil
}

func (m *Membership) Leave(ctx context.Context) error {
	_, err := m.etcdClient.KV.Delete(ctx, m.key())
	if err != nil {
		return err
	}

	return nil
}
