package cbtopology

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/testutils"
	"github.com/couchbase/stellar-gateway/contrib/cbconfig"
)

func TestWatchClusterConfig(t *testing.T) {
	testutils.SkipIfShortTest(t)

	fetcher := cbconfig.NewFetcher(cbconfig.FetcherOptions{
		Host:     "http://" + testutils.TestOpts.HTTPAddrs[0],
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
	testutils.SkipIfShortTest(t)

	fetcher := cbconfig.NewFetcher(cbconfig.FetcherOptions{
		Host:     "http://" + testutils.TestOpts.HTTPAddrs[0],
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
