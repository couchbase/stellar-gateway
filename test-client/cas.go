package gocbps

type Cas uint64

func (cas Cas) toProto() *uint64 {
	if cas == 0 {
		return nil
	}

	protoCas := uint64(cas)
	return &protoCas
}
