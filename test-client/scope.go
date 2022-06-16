package gocbps

type Scope struct {
	bucket    *Bucket
	scopeName string
}

func (s *Scope) Collection(collectionName string) *Collection {
	return &Collection{
		scope:          s,
		collectionName: collectionName,
	}
}
