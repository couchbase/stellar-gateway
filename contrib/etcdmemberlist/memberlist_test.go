package etcdmemberlist

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	etcd "go.etcd.io/etcd/client/v3"
)

var globalTestEtcdClient *etcd.Client
var globalEtcdDisabled bool

func makeTestEtcdClient(t *testing.T) *etcd.Client {
	connectTimeout := 5 * time.Second

	if globalEtcdDisabled {
		t.Fatalf("etcd unavailable: previous connect attempt failed")
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), connectTimeout)

	etcdClient, err := etcd.New(etcd.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: connectTimeout,
	})
	if err != nil {
		globalEtcdDisabled = true
		t.Fatalf("failed to connect to etcd: %s", err)
	}

	_, err = etcdClient.Get(waitCtx, "invalid-key")
	waitCancel()

	if errors.Is(err, context.DeadlineExceeded) {
		globalEtcdDisabled = true
		t.Fatal("failed to connect to etcd: timeout")
	}

	return etcdClient
}

func getTestEtcdClient(t *testing.T) *etcd.Client {
	if globalTestEtcdClient != nil {
		return globalTestEtcdClient
	}

	etcdClient := makeTestEtcdClient(t)

	globalTestEtcdClient = etcdClient
	return etcdClient
}

func genTestPrefix() string {
	return "testing/" + uuid.NewString()
}

func TestEmptyList(t *testing.T) {
	etcdClient := getTestEtcdClient(t)
	prefix := genTestPrefix()

	ml, err := NewMemberList(MemberListOptions{
		EtcdClient: etcdClient,
		KeyPrefix:  prefix,
	})
	if err != nil {
		t.Fatalf("failed to setup member list: %s", err)
	}

	snap, err := ml.Members(context.Background())
	if err != nil {
		t.Fatalf("failed to list members: %s", err)
	}

	if snap.Revision < 0 {
		t.Fatalf("expected a positive revision")
	}

	if len(snap.Members) != 0 {
		t.Fatalf("members list should have been empty")
	}
}

func TestJoinListLeave(t *testing.T) {
	etcdClient := getTestEtcdClient(t)
	prefix := genTestPrefix()

	ml, err := NewMemberList(MemberListOptions{
		EtcdClient: etcdClient,
		KeyPrefix:  prefix,
	})
	if err != nil {
		t.Fatalf("failed to setup member list: %s", err)
	}

	testMeta := []byte("hello")

	mb, err := ml.Join(context.Background(), &JoinOptions{
		MetaData: testMeta,
	})
	if err != nil {
		t.Fatalf("failed to join memberlist: %s", err)
	}

	snap, err := ml.Members(context.Background())
	if err != nil {
		t.Fatalf("failed to list members: %s", err)
	}

	if snap.Revision < 0 {
		t.Fatalf("expected a positive revision")
	}

	if len(snap.Members) != 1 {
		t.Fatalf("members list should have had a single entry")
	}

	if !bytes.Equal(snap.Members[0].MetaData, testMeta) {
		t.Fatalf("membership meta-data was incorrect")
	}

	err = mb.Leave(context.Background())
	if err != nil {
		t.Fatalf("failed to leave memberlist: %s", err)
	}

	snapAfterLeave, err := ml.Members(context.Background())
	if err != nil {
		t.Fatalf("failed to list members after leave: %s", err)
	}

	if snapAfterLeave.Revision <= snap.Revision {
		t.Fatalf("expected a higher revision on newer member list")
	}

	if len(snapAfterLeave.Members) != 0 {
		t.Fatalf("members list should have been empty")
	}
}

func TestDisconnectLeave(t *testing.T) {
	etcdClient := getTestEtcdClient(t)
	prefix := genTestPrefix()

	ml, err := NewMemberList(MemberListOptions{
		EtcdClient: etcdClient,
		KeyPrefix:  prefix,
	})
	if err != nil {
		t.Fatalf("failed to setup member list: %s", err)
	}

	etcdClientDc := makeTestEtcdClient(t)
	mlDc, err := NewMemberList(MemberListOptions{
		EtcdClient: etcdClientDc,
		KeyPrefix:  prefix,
	})
	if err != nil {
		t.Fatalf("failed to setup dc-able member list: %s", err)
	}

	_, err = mlDc.Join(context.Background(), &JoinOptions{
		LeasePeriod: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to join memberlist: %s", err)
	}

	snap, err := ml.Members(context.Background())
	if err != nil {
		t.Fatalf("failed to list members: %s", err)
	}

	if snap.Revision < 0 {
		t.Fatalf("expected a positive revision number")
	}

	if len(snap.Members) != 1 {
		t.Fatalf("members list should have had a single entry")
	}

	etcdClientDc.Close()

	time.Sleep(6 * time.Second)

	snapAfterLeave, err := ml.Members(context.Background())
	if err != nil {
		t.Fatalf("failed to list members after leave: %s", err)
	}

	if snapAfterLeave.Revision <= snap.Revision {
		t.Fatalf("expected a higher revision on updated members list")
	}

	if len(snapAfterLeave.Members) != 0 {
		t.Fatalf("members list should have been empty")
	}
}
