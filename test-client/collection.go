package gocbps

import (
	"context"

	"github.com/couchbase/stellar-nebula/protos"
)

type Collection struct {
	scope          *Scope
	collectionName string
}

func (c *Collection) getClient() (*Client, string, string, string) {
	scope := c.scope
	bucket := scope.bucket
	client := bucket.client
	return client, bucket.bucketName, scope.scopeName, c.collectionName
}

type GetOptions struct {
}

type GetResult struct {
	Content []byte
	Cas     Cas
}

func (c *Collection) Get(ctx context.Context, id string, opts *GetOptions) (*GetResult, error) {
	client, bucketName, scopeName, collName := c.getClient()

	resp, err := client.couchbaseClient.Get(ctx, &protos.GetRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	})
	if err != nil {
		return nil, err
	}

	return &GetResult{
		Content: resp.Content,
		Cas:     Cas(resp.Cas.Value),
	}, nil
}

type UpsertOptions struct {
}

type UpsertResult struct {
	Cas Cas
}

func (c *Collection) Upsert(ctx context.Context, id string, content []byte, opts *UpsertOptions) (*UpsertResult, error) {
	client, bucketName, scopeName, collName := c.getClient()

	resp, err := client.couchbaseClient.Upsert(ctx, &protos.UpsertRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content:        content,
		ContentType:    protos.DocumentContentType_JSON,
	})
	if err != nil {
		return nil, err
	}

	return &UpsertResult{
		Cas: Cas(resp.Cas.Value),
	}, nil
}
