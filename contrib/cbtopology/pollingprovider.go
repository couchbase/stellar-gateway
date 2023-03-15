package cbtopology

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/stellar-gateway/contrib/cbconfig"
	"github.com/couchbase/stellar-gateway/utils/latestonlychannel"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var (
	ErrInconsistentServerData = errors.New("server list data was inconsistent between sources")
)

type PollingProviderOptions struct {
	Fetcher *cbconfig.Fetcher
	Logger  *zap.Logger
}

type PollingProvider struct {
	fetcher *cbconfig.Fetcher
	logger  *zap.Logger
}

var _ Provider = (*PollingProvider)(nil)

func NewPollingProvider(opts PollingProviderOptions) (*PollingProvider, error) {
	p := &PollingProvider{
		fetcher: opts.Fetcher,
		logger:  opts.Logger,
	}

	return p, nil
}

func (p *PollingProvider) parseConfigServers(
	config *cbconfig.TerseConfigJson,
	groupsConfig *cbconfig.ServerGroupConfigJson,
) ([]*Node, error) {
	var servers []*Node
	hostMap := make(map[string]*Node)

	// We parse NodesExt first to get the ordering correct.  This is required
	// when we match the server list to the vbucketMaps...
	for _, nodeJson := range config.NodesExt {
		hostID := fmt.Sprintf("%s:%d", nodeJson.Hostname, nodeJson.Services["mgmt"])

		server := &Node{
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

func (p *PollingProvider) parseVbucketMap(config *cbconfig.VBucketServerMapJson, servers []*Node) ([]*DataNode, error) {
	// TODO(brett19): Add better error handling for missmatch in server lengths

	dataServers := make([]*DataNode, len(config.ServerList))
	for serverIdx := range dataServers {
		dataServers[serverIdx] = &DataNode{
			Node: servers[serverIdx],
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
		Nodes:    servers,
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
		p.logger.Info("configuration fetch was inconsistent, retrying...")
		return p.fetchClusterConfig(ctx, nil)
	}

	// convert to our internal topology representation
	topology, err := p.parseClusterConfig(baseConfig, groups)
	if err != nil {
		return nil, err
	}

	return topology, nil
}

func (p *PollingProvider) watchCluster(ctx context.Context) (<-chan *Topology, error) {
	topology, err := p.fetchClusterConfig(ctx, nil)
	if err != nil {
		return nil, err
	}

	outputCh := make(chan *Topology)

	// start a goroutine to fetch future configs
	go func() {
		outputCh <- topology
		lastTopology := topology

	WatchLoop:
		for {
			topology, err := p.fetchClusterConfig(ctx, nil)
			if err != nil {
				close(outputCh)
				return
			}

			// send this topology if its newer than the previous one
			// TODO(brett19): Rather than just checking revisions, we should check if
			// the contents themselves have actually changed since the last time.  And
			// only if they don't match do we compare revisions to decide.  This will
			// prevent us from triggereing updates from changes we don't care about.
			if topology.Revision > lastTopology.Revision {
				outputCh <- topology
				lastTopology = topology
			}

			select {
			case <-time.After(2500 * time.Millisecond):
			case <-ctx.Done():
				break WatchLoop
			}

		}
	}()

	return latestonlychannel.Wrap(outputCh), nil
}

func (p *PollingProvider) parseBucketConfig(
	config *cbconfig.TerseConfigJson,
	groupsConfig *cbconfig.ServerGroupConfigJson,
) (*Topology, error) {
	servers, err := p.parseConfigServers(config, groupsConfig)
	if err != nil {
		return nil, err
	}

	dataServers, err := p.parseVbucketMap(config.VBucketServerMap, servers)
	if err != nil {
		return nil, err
	}

	return &Topology{
		RevEpoch: uint64(config.RevEpoch),
		Revision: uint64(config.Rev),
		Nodes:    servers,
		VbucketMapping: &VbucketMapping{
			Nodes:       dataServers,
			NumVbuckets: uint(len(config.VBucketServerMap.VBucketMap)),
		},
	}, nil
}

func (p *PollingProvider) watchBucket(ctx context.Context, bucketName string) (<-chan *Topology, error) {
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

	outputCh := make(chan *Topology)

	// start a goroutine to fetch future configs
	go func() {
		outputCh <- topology
		lastTopology := topology

	WatchLoop:
		for {
			// fetch an updated configuration
			config, err := p.fetcher.FetchTerseBucket(ctx, bucketName)
			if err != nil {
				close(outputCh)
				return
			}

			groups, err := p.fetcher.FetchServerGroups(ctx)
			if err != nil {
				close(outputCh)
				return
			}

			// convert to our internal topology representation
			topology, err := p.parseBucketConfig(config, groups)
			if err != nil {
				close(outputCh)
				return
			}

			// send this topology if its newer than the previous one
			// TODO(brett19): Rather than just checking revisions, we should check if
			// the contents themselves have actually changed since the last time.  And
			// only if they don't match do we compare revisions to decide.  This will
			// prevent us from triggereing updates from changes we don't care about.
			if topology.Revision > lastTopology.Revision {
				outputCh <- topology
				lastTopology = topology
			}

			select {
			case <-time.After(2500 * time.Millisecond):
			case <-ctx.Done():
				break WatchLoop
			}

		}
	}()

	return latestonlychannel.Wrap(outputCh), nil
}

func (p *PollingProvider) Watch(ctx context.Context, bucketName string) (<-chan *Topology, error) {
	if bucketName == "" {
		return p.watchCluster(ctx)
	} else {
		return p.watchBucket(ctx, bucketName)
	}
}
