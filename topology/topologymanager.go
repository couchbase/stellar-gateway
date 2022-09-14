package topology

import "fmt"

// TODO(brett19): Implement proper topology management

type TopologyManagerConfig struct {
	LocalHostname string
	LocalPort     int
}

type TopologyManager struct {
	localHostname string
	localPort     int
}

type TopologySnapshot struct {
	Endpoints []string
}

func NewTopologyManager(config TopologyManagerConfig) *TopologyManager {
	return &TopologyManager{
		localHostname: config.LocalHostname,
		localPort:     config.LocalPort,
	}
}

func (t *TopologyManager) GetTopology() *TopologySnapshot {
	return &TopologySnapshot{
		Endpoints: []string{
			fmt.Sprintf("%s:%d", t.localHostname, t.localPort),
		},
	}
}
