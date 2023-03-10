package main

import (
	"context"

	gocbps "github.com/couchbase/stellar-gateway/tools/test-client"
)

type protostellarWrapper struct {
	client *gocbps.Client
	coll   *gocbps.Collection
}

var _ clientWrapper = (*protostellarWrapper)(nil)

func (w *protostellarWrapper) Connect(addr, username, password string) error {
	client, err := gocbps.Connect(addr, &gocbps.ConnectOptions{
		Username: username,
		Password: password,
	})
	if err != nil {
		return err
	}

	b := client.Bucket("default")
	coll := b.DefaultCollection()

	w.client = client
	w.coll = coll
	return nil
}

func (w *protostellarWrapper) Close() {
	w.client.Close()
	w.client = nil
}

func (w *protostellarWrapper) Get(id string) ([]byte, error) {
	res, err := w.coll.Get(context.Background(), id, &gocbps.GetOptions{})
	if err != nil {
		return nil, err
	}

	return res.Content, nil
}

func (w *protostellarWrapper) Upsert(id string, value []byte) error {
	_, err := w.coll.Upsert(context.Background(), id, value, &gocbps.UpsertOptions{})
	if err != nil {
		return err
	}

	return nil
}
