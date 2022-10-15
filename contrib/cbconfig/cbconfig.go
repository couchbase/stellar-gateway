package cbconfig

// TODO(brett19): We should switch to using OpenAPI v3 to define the ns_server REST here...
// Some references:
//   https://github.com/couchbaselabs/cb-swagger/

type VBucketServerMapJson struct {
	HashAlgorithm string   `json:"hashAlgorithm"`
	NumReplicas   int      `json:"numReplicas"`
	ServerList    []string `json:"serverList"`
	VBucketMap    [][]int  `json:"vBucketMap,omitempty"`
}

type ConfigDDocsJson struct {
	URI string `json:"uri,omitempty"`
}

type FullNodeJson struct {
	CouchApiBase string         `json:"couchApiBase,omitempty"`
	Hostname     string         `json:"hostname,omitempty"`
	NodeUUID     string         `json:"nodeUUID,omitempty"`
	Ports        map[string]int `json:"ports,omitempty"`
	Services     []string       `json:"services"`
}

type FullConfigJson struct {
	Name                   string                `json:"name,omitempty"`
	NodeLocator            string                `json:"nodeLocator,omitempty"`
	UUID                   string                `json:"uuid,omitempty"`
	URI                    string                `json:"uri,omitempty"`
	StreamingURI           string                `json:"streamingUri,omitempty"`
	BucketCapabilitiesVer  string                `json:"bucketCapabilitiesVer,omitempty"`
	BucketCapabilities     []string              `json:"bucketCapabilities,omitempty"`
	CollectionsManifestUid string                `json:"collectionsManifestUid,omitempty"`
	DDocs                  *ConfigDDocsJson      `json:"ddocs,omitempty"`
	VBucketServerMap       *VBucketServerMapJson `json:"vBucketServerMap,omitempty"`
	Nodes                  []FullNodeJson        `json:"nodes,omitempty"`
}

type ServerGroupGroupJson struct {
	Name  string         `json:"name,omitempty"`
	Nodes []FullNodeJson `json:"nodes"`
}

type ServerGroupConfigJson struct {
	Groups []ServerGroupGroupJson `json:"groups"`
}

type TerseNodeJson struct {
	CouchApiBase string         `json:"couchApiBase,omitempty"`
	Hostname     string         `json:"hostname,omitempty"`
	Ports        map[string]int `json:"ports,omitempty"`
}

type TerseExtNodeJson struct {
	Services map[string]int `json:"services,omitempty"`
	ThisNode bool           `json:"thisNode,omitempty"`
	Hostname string         `json:"hostname,omitempty"`
}

type TerseConfigJson struct {
	Rev                    int                   `json:"rev,omitempty"`
	RevEpoch               int                   `json:"revEpoch,omitempty"`
	Name                   string                `json:"name,omitempty"`
	NodeLocator            string                `json:"nodeLocator,omitempty"`
	UUID                   string                `json:"uuid,omitempty"`
	URI                    string                `json:"uri,omitempty"`
	StreamingURI           string                `json:"streamingUri,omitempty"`
	BucketCapabilitiesVer  string                `json:"bucketCapabilitiesVer,omitempty"`
	BucketCapabilities     []string              `json:"bucketCapabilities,omitempty"`
	CollectionsManifestUid string                `json:"collectionsManifestUid,omitempty"`
	DDocs                  *ConfigDDocsJson      `json:"ddocs,omitempty"`
	VBucketServerMap       *VBucketServerMapJson `json:"vBucketServerMap,omitempty"`
	Nodes                  []TerseNodeJson       `json:"nodes,omitempty"`
	NodesExt               []TerseExtNodeJson    `json:"nodesExt,omitempty"`
	ClusterCapabilitiesVer []int                 `json:"clusterCapabilitiesVer"`
	ClusterCapabilities    map[string][]string   `json:"clusterCapabilities"`
}
