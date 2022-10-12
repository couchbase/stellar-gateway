package clustering

type Endpoint struct {
	NodeID      string
	ServerGroup string

	AdvertiseAddr string
	AdvertisePort int
}

type Snapshot struct {
	RevEpoch uint64
	Revision uint64

	Endpoints []Endpoint
}
