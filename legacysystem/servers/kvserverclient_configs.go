package servers

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/stellar-nebula/contrib/cbconfig"
)

func (c *KvServerClient) handleCmdGetClusterConfigReq(pak *memd.Packet) {
	if !c.validatePacket(pak, 0) {
		return
	}

	bucketName := c.selectedBucket
	bucketUuid := "a4cbbc10b7b9ab21244b47c8d0c8fe5a"
	bucketNameWithUuid := bucketName + "?" + bucketUuid

	// TODO(brett19): Generate real configurations...
	config := &cbconfig.TerseConfigJson{}

	config.RevEpoch = 1
	config.Rev = 1

	nodeExt := cbconfig.TerseExtNodeJson{}
	{
		nodeExt.Hostname = "127.0.0.1"
		nodeExt.ThisNode = true
		nodeExt.Services = map[string]int{
			"mgmt": 8091,
			"kv":   11210,
		}
	}
	config.NodesExt = append(config.NodesExt, nodeExt)

	config.ClusterCapabilitiesVer = []int{1, 0}
	config.ClusterCapabilities = map[string][]string{
		"n1ql": {"enhancedPreparedStatements"},
	}

	if c.selectedBucket != "" {
		config.Name = bucketName

		config.BucketCapabilitiesVer = ""
		config.BucketCapabilities = []string{
			"collections",
			"durableWrite",
			"tombstonedUserXAttrs",
			"couchapi",
			"subdoc.ReplaceBodyWithXattr",
			"subdoc.DocumentMacroSupport",
			"dcp",
			"cbhello",
			"touch",
			"cccp",
			"xdcrCheckpointing",
			"nodesExt",
			"xattr",
		}

		node := cbconfig.TerseNodeJson{}
		{
			node.CouchApiBase = fmt.Sprintf("http://127.0.0.1:8092/%s", url.PathEscape(bucketNameWithUuid))
			node.Hostname = "127.0.0.1:8091"
			node.Ports = map[string]int{
				"direct": 11210,
			}
		}
		config.Nodes = []cbconfig.TerseNodeJson{
			node,
		}

		config.NodeLocator = "vbucket"
		config.VBucketServerMap = &cbconfig.VBucketServerMapJson{
			HashAlgorithm: "CRC",
			NumReplicas:   0,
			ServerList:    []string{"127.0.0.1:11210"},
			VBucketMap: [][]int{
				// pretend like we have 4 vbuckets...
				0: {0},
				1: {0},
				2: {0},
				3: {0},
			},
		}
	}

	configBytes, err := json.Marshal(config)
	if err != nil {
		c.sendInternalError(pak, err)
		return
	}

	c.sendBasicReply(pak, memd.StatusSuccess, nil, configBytes, nil)
}
