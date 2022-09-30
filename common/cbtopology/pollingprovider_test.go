package cbtopology

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/stellar-nebula/common/cbconfig"
)

func TestWatchClusterConfig(t *testing.T) {
	fetcher := cbconfig.NewFetcher(cbconfig.FetcherOptions{
		Host:     "http://192.168.0.100:8091",
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

	clusterConfigs, err := poller.WatchCluster(cancelCtx)
	if err != nil {
		t.Fatalf("failed to watch cluster configs: %s", err)
	}

	clusterConfig := <-clusterConfigs
	if len(clusterConfig.Servers) == 0 {
		t.Fatalf("failed to parse servers")
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
	fetcher := cbconfig.NewFetcher(cbconfig.FetcherOptions{
		Host:     "http://192.168.0.100:8091",
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

	bucketConfigs, err := poller.WatchBucket(cancelCtx, "default")
	if err != nil {
		t.Fatalf("failed to watch bucket configs: %s", err)
	}

	bucketConfig := <-bucketConfigs
	if len(bucketConfig.Servers) == 0 {
		t.Fatalf("failed to parse servers")
	}
	if len(bucketConfig.DataServers) == 0 {
		t.Fatalf("failed to parse vbucket data")
	}
	if len(bucketConfig.DataServers[0].Vbuckets) <= 0 {
		t.Fatalf("failed to parse server vbuckets")
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
