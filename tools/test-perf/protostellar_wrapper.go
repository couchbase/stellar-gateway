package main

import (
	"context"
	"sync/atomic"

	gocbps "github.com/couchbase/stellar-gateway/tools/test-client"
)

type protostellarWrapper struct {
	clients []*gocbps.Client
	colls   []*gocbps.Collection
	iterIdx uint64
}

var _ clientWrapper = (*protostellarWrapper)(nil)

func (w *protostellarWrapper) Connect(addr, username, password string) error {
	NUM_CLIENTS := 64

	for i := 0; i < NUM_CLIENTS; i++ {
		client, err := gocbps.Connect(addr, &gocbps.ConnectOptions{
			Username: username,
			Password: password,
		})
		if err != nil {
			return err
		}

		b := client.Bucket("default")
		coll := b.DefaultCollection()

		w.clients = append(w.clients, client)
		w.colls = append(w.colls, coll)
	}

	return nil
}

func (w *protostellarWrapper) Close() {
	for _, client := range w.clients {
		_ = client.Close()
	}
	w.clients = nil
	w.colls = nil
}

func (w *protostellarWrapper) getColl() *gocbps.Collection {
	iterIdx := atomic.AddUint64(&w.iterIdx, 1)
	iterIdx = iterIdx % uint64(len(w.colls))
	coll := w.colls[iterIdx]
	return coll
}

func (w *protostellarWrapper) Get(id string) ([]byte, error) {
	res, err := w.getColl().Get(context.Background(), id, &gocbps.GetOptions{})
	if err != nil {
		return nil, err
	}

	return res.Content, nil
}

func (w *protostellarWrapper) Upsert(id string, value []byte) error {
	_, err := w.getColl().Upsert(context.Background(), id, value, &gocbps.UpsertOptions{})
	if err != nil {
		return err
	}

	return nil
}
