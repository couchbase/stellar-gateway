/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package cbtopology

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/testutilsint"
	"github.com/couchbase/stellar-gateway/contrib/cbconfig"
)

func TestWatchClusterConfig(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	fetcher := cbconfig.NewFetcher(cbconfig.FetcherOptions{
		Host:     "http://" + testutilsint.TestOpts.HTTPAddrs[0],
		Username: "Administrator",
		Password: "password",
	})

	poller, err := NewPollingProvider(PollingProviderOptions{
		Fetcher: fetcher,
	})
	if err != nil {
		t.Fatalf("failed to create poller: %s", err)
	}

	cancelCtx, cancelFn := context.WithCancel(context.Background())

	clusterConfigs, err := poller.Watch(cancelCtx, "")
	if err != nil {
		t.Fatalf("failed to watch cluster configs: %s", err)
	}

	clusterConfig := <-clusterConfigs
	if len(clusterConfig.Nodes) == 0 {
		t.Fatalf("failed to parse nodes")
	}

	cancelFn()

	waitCh := time.After(100 * time.Millisecond)

waitCancelLoop:
	for {
		select {
		case _, ok := <-clusterConfigs:
			if !ok {
				// closed
				break waitCancelLoop
			}
		case <-waitCh:
			t.Fatalf("failed to close the stream")
		}
	}
}

func TestWatchBucketConfig(t *testing.T) {
	testutilsint.SkipIfShortTest(t)

	fetcher := cbconfig.NewFetcher(cbconfig.FetcherOptions{
		Host:     "http://" + testutilsint.TestOpts.HTTPAddrs[0],
		Username: "Administrator",
		Password: "password",
	})

	poller, err := NewPollingProvider(PollingProviderOptions{
		Fetcher: fetcher,
	})
	if err != nil {
		t.Fatalf("failed to create poller: %s", err)
	}

	cancelCtx, cancelFn := context.WithCancel(context.Background())

	bucketConfigs, err := poller.Watch(cancelCtx, "default")
	if err != nil {
		t.Fatalf("failed to watch bucket configs: %s", err)
	}

	bucketConfig := <-bucketConfigs
	if len(bucketConfig.Nodes) == 0 {
		t.Fatalf("failed to parse nodes")
	}
	if len(bucketConfig.VbucketMapping.Nodes) == 0 {
		t.Fatalf("failed to parse vbucket data")
	}
	if len(bucketConfig.VbucketMapping.Nodes[0].Vbuckets) <= 0 {
		t.Fatalf("failed to parse node vbuckets")
	}

	cancelFn()

	waitCh := time.After(100 * time.Millisecond)
waitCancelLoop:
	for {
		select {
		case _, ok := <-bucketConfigs:
			if !ok {
				// closed
				break waitCancelLoop
			}
		case <-waitCh:
			t.Fatalf("failed to close the stream")
		}
	}
}
