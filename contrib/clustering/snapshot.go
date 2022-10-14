package clustering

type Member struct {
	MemberID string
	MetaData []byte
}

type Snapshot struct {
	Revision []uint64
	Members  []*Member
}
