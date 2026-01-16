package main

import (
	"context"
	"fmt"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbaselabs/gocbconnstr/v2"
)

type directWrapper struct {
	client *gocbcorex.Agent
}

var _ clientWrapper = (*directWrapper)(nil)

func (w *directWrapper) Connect(addr, username, password string) error {
	baseSpec, err := gocbconnstr.Parse(addr)
	if err != nil {
		return err
	}

	spec, err := gocbconnstr.Resolve(baseSpec)
	if err != nil {
		return err
	}

	var httpHosts []string
	for _, specHost := range spec.HttpHosts {
		httpHosts = append(httpHosts, fmt.Sprintf("%s:%d", specHost.Host, specHost.Port))
	}

	var memdHosts []string
	for _, specHost := range spec.MemdHosts {
		memdHosts = append(memdHosts, fmt.Sprintf("%s:%d", specHost.Host, specHost.Port))
	}

	agent, err := gocbcorex.CreateAgent(context.Background(), gocbcorex.AgentOptions{
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: httpHosts,
			MemdAddrs: memdHosts,
		},
		BucketName: "default",
	})
	if err != nil {
		return err
	}

	w.client = agent
	return nil
}

func (w *directWrapper) Close() {
	_ = w.client.Close()
	w.client = nil
}

func (w *directWrapper) Get(id string) ([]byte, error) {
	ctx, cancel := w.kvDeadline()
	defer cancel()
	res, err := w.client.Get(
		ctx,
		&gocbcorex.GetOptions{
			Key:            []byte(id),
			ScopeName:      "_default",
			CollectionName: "_default",
		},
	)
	if err != nil {
		return nil, err
	}

	return res.Value, nil
}

func (w *directWrapper) Upsert(id string, value []byte) error {
	ctx, cancel := w.kvDeadline()
	defer cancel()

	_, err := w.client.Upsert(ctx, &gocbcorex.UpsertOptions{
		Key:            []byte(id),
		ScopeName:      "_default",
		CollectionName: "_default",
		Value:          value,
	})
	if err != nil {
		return err
	}

	return nil
}

func (w *directWrapper) kvDeadline() (context.Context, context.CancelFunc) {
	return context.WithDeadline(context.Background(), time.Now().Add(2500*time.Millisecond))
}
