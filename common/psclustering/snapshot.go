package psclustering

type Member struct {
	MemberID      string `json:"-"`
	ServerGroup   string `json:"server_group"`
	AdvertiseAddr string `json:"advertise_addr"`
	AdvertisePort int    `json:"advertise_port"`
}

type Snapshot struct {
	Revision []uint64
	Members  []*Member
}
