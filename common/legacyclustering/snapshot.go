package legacyclustering

type ServicePorts struct {
	Mgmt  int `json:"mgmt"`
	Kv    int `json:"kv"`
	Query int `json:"query"`

	MgmtTls  int `json:"mgmt_tls"`
	KvTls    int `json:"kv_tls"`
	QueryTls int `json:"query_tls"`
}

type Member struct {
	MemberID      string       `json:"-"`
	ServerGroup   string       `json:"server_group"`
	AdvertiseAddr string       `json:"advertise_addr"`
	AdvertisePort ServicePorts `json:"advertise_ports"`
}

type Snapshot struct {
	Revision []uint64
	Members  []*Member
}
