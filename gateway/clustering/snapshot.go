package clustering

// The JSON representation of this data is intentionally terse in order to allow
// it to potentially fit easily in UDP gossip messages.

type ServicePorts struct {
	SD int `json:"s"`
	PS int `json:"p"`
}

type Member struct {
	MemberID       string       `json:"-"`
	ServerGroup    string       `json:"sg,omitempty"`
	AdvertiseAddr  string       `json:"aa,omitempty"`
	AdvertisePorts ServicePorts `json:"ap,omitempty"`
}

type Snapshot struct {
	Revision []uint64
	Members  []*Member
}
