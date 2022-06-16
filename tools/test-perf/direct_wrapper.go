package main

import (
	"encoding/json"
	"time"

	"github.com/couchbase/gocb/v2"
)

type directWrapper struct {
	clientWrapper

	client *gocb.Cluster
	coll   *gocb.Collection
}

func (w *directWrapper) Connect(addr, username, password string) error {
	client, err := gocb.Connect(addr, gocb.ClusterOptions{
		Username: username,
		Password: password,
	})
	if err != nil {
		return err
	}

	b := client.Bucket("default")

	err = b.WaitUntilReady(10*time.Second, &gocb.WaitUntilReadyOptions{
		ServiceTypes: []gocb.ServiceType{
			gocb.ServiceTypeKeyValue,
		},
	})
	if err != nil {
		return err
	}

	coll := b.DefaultCollection()

	w.client = client
	w.coll = coll
	return nil
}

func (w *directWrapper) Close() {
	w.client.Close(&gocb.ClusterCloseOptions{})

	w.client = nil
	w.coll = nil
}

func (w *directWrapper) Get(id string) ([]byte, error) {
	res, err := w.coll.Get(id, &gocb.GetOptions{})
	if err != nil {
		return nil, err
	}

	var content json.RawMessage
	err = res.Content(&content)
	if err != nil {
		return nil, err
	}

	return content, nil
}

func (w *directWrapper) Upsert(id string, value []byte) error {
	content := json.RawMessage(value)

	_, err := w.coll.Upsert(id, content, &gocb.UpsertOptions{})
	if err != nil {
		return err
	}

	return nil
}
