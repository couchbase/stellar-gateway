/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

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
	})
	if err != nil {
		return err
	}

	w.client = agent
	return nil
}

func (w *directWrapper) Close() {
	w.client.Close()

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
