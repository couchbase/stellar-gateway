package gocbps

type Bucket struct {
	client     *Client
	bucketName string
}

func (b *Bucket) Scope(scopeName string) *Scope {
	return &Scope{
		bucket:    b,
		scopeName: scopeName,
	}
}

func (b *Bucket) Collection(collectionName string) *Collection {
	return b.Scope("_default").Collection(collectionName)
}

func (b *Bucket) DefaultCollection() *Collection {
	return b.Scope("_default").Collection("_default")
}
