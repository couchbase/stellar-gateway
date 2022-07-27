package gocbps

import (
	"context"
	"github.com/couchbase/stellar-nebula/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
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
	if opts == nil {
		opts = &GetOptions{}
	}
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

type ExistsOptions struct {
}

type ExistsResult struct {
	Exists bool
	Cas    Cas
}

func (c *Collection) Exists(ctx context.Context, id string, opts *ExistsOptions) (*ExistsResult, error) {
	if opts == nil {
		opts = &ExistsOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	resp, err := client.couchbaseClient.Exists(ctx, &protos.ExistsRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	})
	if err != nil {
		return nil, err
	}

	return &ExistsResult{
		Exists: resp.Result,
		Cas:    Cas(resp.Cas.Value),
	}, nil
}

type GetAndTouchOptions struct {
}

func (c *Collection) GetAndTouch(ctx context.Context, id string, expiry time.Time, opts *GetAndTouchOptions) (*GetResult, error) {
	if opts == nil {
		opts = &GetAndTouchOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	resp, err := client.couchbaseClient.GetAndTouch(ctx, &protos.GetAndTouchRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry:         timestamppb.New(expiry),
	})
	if err != nil {
		return nil, err
	}

	return &GetResult{
		Content: resp.Content,
		Cas:     Cas(resp.Cas.Value),
	}, nil
}

type GetAndLockOptions struct {
}

func (c *Collection) GetAndLock(ctx context.Context, id string, lockTime time.Duration, opts *GetAndLockOptions) (*GetResult, error) {
	if opts == nil {
		opts = &GetAndLockOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	resp, err := client.couchbaseClient.GetAndLock(ctx, &protos.GetAndLockRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		LockTime:       uint32(lockTime / time.Second),
	})
	if err != nil {
		return nil, err
	}

	return &GetResult{
		Content: resp.Content,
		Cas:     Cas(resp.Cas.Value),
	}, nil
}

type UnlockOptions struct {
}

func (c *Collection) Unlock(ctx context.Context, id string, cas Cas, opts *UnlockOptions) error {
	if opts == nil {
		opts = &UnlockOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	_, err := client.couchbaseClient.Unlock(ctx, &protos.UnlockRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Cas:            cas.toProto(),
	})
	if err != nil {
		return err
	}

	return nil
}

type UpsertOptions struct {
	DurabilityLevel DurabilityLevel
}

type MutationResult struct {
	Cas Cas
}

func (c *Collection) Upsert(ctx context.Context, id string, content []byte, opts *UpsertOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &UpsertOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	resp, err := client.couchbaseClient.Upsert(ctx, &protos.UpsertRequest{
		BucketName:      bucketName,
		ScopeName:       scopeName,
		CollectionName:  collName,
		Key:             id,
		Content:         content,
		ContentType:     protos.DocumentContentType_JSON,
		DurabilityLevel: opts.DurabilityLevel.toProto(),
	})
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas: Cas(resp.Cas.Value),
	}, nil
}

type InsertOptions struct {
	DurabilityLevel DurabilityLevel
}

func (c *Collection) Insert(ctx context.Context, id string, content []byte, opts *InsertOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &InsertOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	resp, err := client.couchbaseClient.Insert(ctx, &protos.InsertRequest{
		BucketName:      bucketName,
		ScopeName:       scopeName,
		CollectionName:  collName,
		Key:             id,
		Content:         content,
		ContentType:     protos.DocumentContentType_JSON,
		DurabilityLevel: opts.DurabilityLevel.toProto(),
	})
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas: Cas(resp.Cas.Value),
	}, nil
}

type ReplaceOptions struct {
	DurabilityLevel DurabilityLevel
}

func (c *Collection) Replace(ctx context.Context, id string, content []byte, opts *ReplaceOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &ReplaceOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	resp, err := client.couchbaseClient.Replace(ctx, &protos.ReplaceRequest{
		BucketName:      bucketName,
		ScopeName:       scopeName,
		CollectionName:  collName,
		Key:             id,
		Content:         content,
		ContentType:     protos.DocumentContentType_JSON,
		DurabilityLevel: opts.DurabilityLevel.toProto(),
	})
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas: Cas(resp.Cas.Value),
	}, nil
}

type RemoveOptions struct {
	DurabilityLevel DurabilityLevel
}

func (c *Collection) Remove(ctx context.Context, id string, opts *RemoveOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &RemoveOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	resp, err := client.couchbaseClient.Remove(ctx, &protos.RemoveRequest{
		BucketName:      bucketName,
		ScopeName:       scopeName,
		CollectionName:  collName,
		Key:             id,
		DurabilityLevel: opts.DurabilityLevel.toProto(),
	})
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas: Cas(resp.Cas.Value),
	}, nil
}

type TouchOptions struct {
}

func (c *Collection) Touch(ctx context.Context, id string, expiry time.Time, opts *TouchOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &TouchOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	resp, err := client.couchbaseClient.Touch(ctx, &protos.TouchRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry:         timestamppb.New(expiry),
	})
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas: Cas(resp.Cas.Value),
	}, nil
}
