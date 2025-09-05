package testutils

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/stretchr/testify/require"
)

type NodeTarget struct {
	Hostname       string
	NsPort         uint16
	QueryPort      uint16
	SearchPort     uint16
	GSIPort        uint16
	IsOrchestrator bool
}

type NodeTargetList []*NodeTarget

func getOrchestratorNsAddr(t *testing.T) string {
	clusterInfo, err := getTestMgmt().GetTerseClusterInfo(context.Background(), &cbmgmtx.GetTerseClusterConfigOptions{})
	require.NoError(t, err)

	if clusterInfo.Orchestrator == "undefined" {
		// sometimes ns_server will return an orchestrator otp name of "undefined".
		// so we loop here to wait until ns_server figures out who the orchestrator is.
		time.Sleep(1 * time.Second)
		return getOrchestratorNsAddr(t)
	}

	config, err := getTestMgmt().GetClusterConfig(context.Background(), &cbmgmtx.GetClusterConfigOptions{})
	require.NoError(t, err)

	var otpNodes []string
	for _, node := range config.Nodes {
		if node.OTPNode == clusterInfo.Orchestrator {
			return node.Hostname
		}
		otpNodes = append(otpNodes, node.OTPNode)
	}

	require.Fail(t, "failed to find orchestrator nsaddr", "found %s nodes in config, cluster info: %v", strings.Join(otpNodes, ""), clusterInfo)
	return ""
}

func GetTestNodes(t *testing.T) NodeTargetList {
	config, err := getTestMgmt().GetTerseClusterConfig(context.Background(), &cbmgmtx.GetTerseClusterConfigOptions{})
	require.NoError(t, err)

	srcUrl, _ := url.Parse(getTestMgmt().Endpoint)
	srcHostname := srcUrl.Hostname()
	orchestratorNsAddr := getOrchestratorNsAddr(t)

	var nodes []*NodeTarget
	for _, nodeExt := range config.NodesExt {
		hostname := nodeExt.Hostname
		if hostname == "" {
			hostname = srcHostname
		}

		nsAddress := fmt.Sprintf("%s:%d", hostname, nodeExt.Services.Mgmt)

		nodes = append(nodes, &NodeTarget{
			Hostname:       hostname,
			NsPort:         nodeExt.Services.Mgmt,
			QueryPort:      nodeExt.Services.N1ql,
			SearchPort:     nodeExt.Services.Fts,
			GSIPort:        nodeExt.Services.GSI,
			IsOrchestrator: nsAddress == orchestratorNsAddr,
		})
	}

	return nodes
}
