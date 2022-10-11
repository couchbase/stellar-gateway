package cbtopology

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/couchbase/stellar-nebula/common/cbconfig"
	"github.com/couchbase/stellar-nebula/utils"
	"golang.org/x/exp/slices"
)

var (
	ErrInconsistentServerData = errors.New("server list data was inconsistent between sources")
)

type PollingProviderOptions struct {
	Fetcher *cbconfig.Fetcher
}

type PollingProvider struct {
	fetcher *cbconfig.Fetcher
}

func NewPollingProvider(opts PollingProviderOptions) (*PollingProvider, error) {
	p := &PollingProvider{
		fetcher: opts.Fetcher,
	}

	return p, nil
}

func (p *PollingProvider) parseConfigServers(
	config *cbconfig.TerseConfigJson,
	groupsConfig *cbconfig.ServerGroupConfigJson,
) ([]*Server, error) {
	var servers []*Server
	hostMap := make(map[string]*Server)

	// We parse NodesExt first to get the ordering correct.  This is required
	// when we match the server list to the vbucketMaps...
	for _, nodeJson := range config.NodesExt {
		hostID := fmt.Sprintf("%s:%d", nodeJson.Hostname, nodeJson.Services["mgmt"])

		server := &Server{
			HostID:      hostID,
			NodeID:      "",
			ServerGroup: "",

			HasMgmt:      false,
			HasKv:        false,
			HasViews:     false,
			HasQuery:     false,
			HasAnalytics: false,
			HasSearch:    false,
		}
		servers = append(servers, server)
		hostMap[hostID] = server
	}

	for _, groupJson := range groupsConfig.Groups {
		for _, nodeJson := range groupJson.Nodes {
			hostID := nodeJson.Hostname
			server := hostMap[hostID]
			if server == nil {
				return nil, ErrInconsistentServerData
			}

			server.NodeID = nodeJson.NodeUUID
			server.ServerGroup = groupJson.Name

			server.HasMgmt = true
			server.HasKv = slices.Contains(nodeJson.Services, "kv")
			server.HasViews = true
			server.HasQuery = slices.Contains(nodeJson.Services, "n1ql")
			server.HasAnalytics = slices.Contains(nodeJson.Services, "cbas")
			server.HasSearch = slices.Contains(nodeJson.Services, "fts")
		}
	}

	return servers, nil
}

func (p *PollingProvider) parseVbucketMap(config *cbconfig.VBucketServerMapJson, servers []*Server) ([]*DataServer, error) {
	// TODO(brett19): Add better error handling for missmatch in server lengths

	dataServers := make([]*DataServer, len(config.ServerList))
	for serverIdx := range dataServers {
		dataServers[serverIdx] = &DataServer{
			Server: servers[serverIdx],
		}
	}

	for vbIdx, vbReplicas := range config.VBucketMap {
		for replicaIdx, serverIdx := range vbReplicas {
			if serverIdx < 0 {
				// ignore vbuckets with no server assigned
				continue
			}

			if replicaIdx == 0 {
				dataServers[serverIdx].Vbuckets =
					append(dataServers[serverIdx].Vbuckets, vbIdx)
			} else {
				dataServers[serverIdx].VbucketReplicas =
					append(dataServers[serverIdx].VbucketReplicas, vbIdx)
			}
		}
	}

	return dataServers, nil
}

func (p *PollingProvider) parseClusterConfig(
	config *cbconfig.TerseConfigJson,
	groupsConfig *cbconfig.ServerGroupConfigJson,
) (*Topology, error) {
	servers, err := p.parseConfigServers(config, groupsConfig)
	if err != nil {
		return nil, err
	}

	return &Topology{
		RevEpoch: uint64(config.RevEpoch),
		Revision: uint64(config.Rev),
		Servers:  servers,
	}, nil
}

func (p *PollingProvider) fetchClusterConfig(ctx context.Context, baseConfig *cbconfig.TerseConfigJson) (*Topology, error) {
	// fetch node services...
	if baseConfig == nil {
		config, err := p.fetcher.FetchNodeServices(ctx)
		if err != nil {
			return nil, err
		}

		baseConfig = config
	}

	// fetch group/uuid information
	groups, err := p.fetcher.FetchServerGroups(ctx)
	if err != nil {
		return nil, err
	}

	// fetch nodeServices again to confirm the topology hasn't changed
	refetchedConfig, err := p.fetcher.FetchNodeServices(ctx)
	if err != nil {
		return nil, err
	}

	// if the newly fetched config has changed, we need to refetch the config
	// TODO(brett19): fast changing servers could make this retry a lot, we should
	// check if the data is consistent instead of just checking revisions.
	if refetchedConfig.Rev != baseConfig.Rev ||
		refetchedConfig.RevEpoch != baseConfig.RevEpoch {
		log.Printf("configuration fetch was inconsistent, retrying...")
		return p.fetchClusterConfig(ctx, nil)
	}

	// convert to our internal topology representation
	topology, err := p.parseClusterConfig(baseConfig, groups)
	if err != nil {
		return nil, err
	}

	return topology, nil
}

func (p *PollingProvider) WatchCluster(ctx context.Context) (<-chan *Topology, error) {
	topology, err := p.fetchClusterConfig(ctx, nil)
	if err != nil {
		return nil, err
	}

	// send the first output to the output, it's buffered to let it hold the
	// initial configuration...
	inputCh, outputCh := utils.LatestOnlyChannel[*Topology]()
	inputCh <- topology

	lastTopology := topology

	// start a goroutine to fetch future configs
	go func() {
	WatchLoop:
		for {
			topology, err := p.fetchClusterConfig(ctx, nil)
			if err != nil {
				close(inputCh)
				return
			}

			// send this topology if its newer than the previous one
			// TODO(brett19): Rather than just checking revisions, we should check if
			// the contents themselves have actually changed since the last time.  And
			// only if they don't match do we compare revisions to decide.  This will
			// prevent us from triggereing updates from changes we don't care about.
			if topology.Revision > lastTopology.Revision {
				inputCh <- topology
				lastTopology = topology
			}

			select {
			case <-time.After(2500 * time.Millisecond):
			case <-ctx.Done():
				break WatchLoop
			}

		}
	}()

	return outputCh, nil
}

func (p *PollingProvider) parseBucketConfig(
	config *cbconfig.TerseConfigJson,
	groupsConfig *cbconfig.ServerGroupConfigJson,
) (*BucketTopology, error) {
	servers, err := p.parseConfigServers(config, groupsConfig)
	if err != nil {
		return nil, err
	}

	dataServers, err := p.parseVbucketMap(config.VBucketServerMap, servers)
	if err != nil {
		return nil, err
	}

	return &BucketTopology{
		RevEpoch:    uint64(config.RevEpoch),
		Revision:    uint64(config.Rev),
		Servers:     servers,
		DataServers: dataServers,
	}, nil
}

func (p *PollingProvider) WatchBucket(ctx context.Context, bucketName string) (<-chan *BucketTopology, error) {
	// fetch the first version
	config, err := p.fetcher.FetchTerseBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	groups, err := p.fetcher.FetchServerGroups(ctx)
	if err != nil {
		return nil, err
	}

	// convert to our internal topology representation
	topology, err := p.parseBucketConfig(config, groups)
	if err != nil {
		return nil, err
	}

	// send the first output to the output, it's buffered to let it hold the
	// initial configuration...
	inputCh, outputCh := utils.LatestOnlyChannel[*BucketTopology]()
	inputCh <- topology

	lastTopology := topology

	// start a goroutine to fetch future configs
	go func() {
	WatchLoop:
		for {
			// fetch an updated configuration
			config, err := p.fetcher.FetchTerseBucket(ctx, bucketName)
			if err != nil {
				close(inputCh)
				return
			}

			groups, err := p.fetcher.FetchServerGroups(ctx)
			if err != nil {
				close(inputCh)
				return
			}

			// convert to our internal topology representation
			topology, err := p.parseBucketConfig(config, groups)
			if err != nil {
				close(inputCh)
				return
			}

			// send this topology if its newer than the previous one
			// TODO(brett19): Rather than just checking revisions, we should check if
			// the contents themselves have actually changed since the last time.  And
			// only if they don't match do we compare revisions to decide.  This will
			// prevent us from triggereing updates from changes we don't care about.
			if topology.Revision > lastTopology.Revision {
				inputCh <- topology
				lastTopology = topology
			}

			select {
			case <-time.After(2500 * time.Millisecond):
			case <-ctx.Done():
				break WatchLoop
			}

		}
	}()

	return outputCh, nil
}
