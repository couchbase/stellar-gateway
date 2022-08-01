package gocbps

import (
	"context"
	"time"

	"github.com/couchbase/stellar-nebula/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	req := &protos.GetRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	}

	resp, err := client.couchbaseClient.Get(ctx, req)
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

	req := &protos.ExistsRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	}

	resp, err := client.couchbaseClient.Exists(ctx, req)
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

func (c *Collection) GetAndTouch(ctx context.Context, id string, expiry time.Duration, opts *GetAndTouchOptions) (*GetResult, error) {
	if opts == nil {
		opts = &GetAndTouchOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	req := &protos.GetAndTouchRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry:         durationToTimestamp(expiry),
	}

	resp, err := client.couchbaseClient.GetAndTouch(ctx, req)
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

	req := &protos.GetAndLockRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		LockTime:       uint32(lockTime / time.Second),
	}

	resp, err := client.couchbaseClient.GetAndLock(ctx, req)
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

	req := &protos.UnlockRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Cas:            cas.toProto(),
	}

	_, err := client.couchbaseClient.Unlock(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

type UpsertOptions struct {
	DurabilityLevel DurabilityLevel
	PersistTo       uint32
	ReplicateTo     uint32
	Expiry          time.Duration
}

type MutationResult struct {
	Cas Cas
}

func (c *Collection) Upsert(ctx context.Context, id string, content []byte, opts *UpsertOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &UpsertOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	req := &protos.UpsertRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content:        content,
		ContentType:    protos.DocumentContentType_JSON,
		Expiry:         durationToTimestamp(opts.Expiry),
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &protos.UpsertRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &protos.UpsertRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &protos.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}
	resp, err := client.couchbaseClient.Upsert(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas: Cas(resp.Cas.Value),
	}, nil
}

type InsertOptions struct {
	DurabilityLevel DurabilityLevel
	PersistTo       uint32
	ReplicateTo     uint32
	Expiry          time.Duration
}

func (c *Collection) Insert(ctx context.Context, id string, content []byte, opts *InsertOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &InsertOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	req := &protos.InsertRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content:        content,
		ContentType:    protos.DocumentContentType_JSON,
		Expiry:         durationToTimestamp(opts.Expiry),
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &protos.InsertRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &protos.InsertRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &protos.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}

	resp, err := client.couchbaseClient.Insert(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas: Cas(resp.Cas.Value),
	}, nil
}

type ReplaceOptions struct {
	DurabilityLevel DurabilityLevel
	PersistTo       uint32
	ReplicateTo     uint32
	Expiry          time.Duration
	Cas             Cas
}

func (c *Collection) Replace(ctx context.Context, id string, content []byte, opts *ReplaceOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &ReplaceOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	var cas *protos.Cas
	if opts.Cas > 0 {
		cas = &protos.Cas{
			Value: uint64(opts.Cas),
		}
	}

	req := &protos.ReplaceRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content:        content,
		ContentType:    protos.DocumentContentType_JSON,
		Expiry:         durationToTimestamp(opts.Expiry),
		Cas:            cas,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &protos.ReplaceRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &protos.ReplaceRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &protos.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}

	resp, err := client.couchbaseClient.Replace(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas: Cas(resp.Cas.Value),
	}, nil
}

type RemoveOptions struct {
	DurabilityLevel DurabilityLevel
	PersistTo       uint32
	ReplicateTo     uint32
	Cas             Cas
}

func (c *Collection) Remove(ctx context.Context, id string, opts *RemoveOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &RemoveOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	var cas *protos.Cas
	if opts.Cas > 0 {
		cas = &protos.Cas{
			Value: uint64(opts.Cas),
		}
	}

	req := &protos.RemoveRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Cas:            cas,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &protos.RemoveRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &protos.RemoveRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &protos.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}

	resp, err := client.couchbaseClient.Remove(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas: Cas(resp.Cas.Value),
	}, nil
}

type TouchOptions struct {
}

func (c *Collection) Touch(ctx context.Context, id string, expiry time.Duration, opts *TouchOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &TouchOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	req := &protos.TouchRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry:         durationToTimestamp(expiry),
	}

	resp, err := client.couchbaseClient.Touch(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas: Cas(resp.Cas.Value),
	}, nil
}

// Do we actually need this?
func durationToTimestamp(dura time.Duration) *timestamppb.Timestamp {
	// If the duration is 0, that indicates never-expires
	if dura == 0 {
		return nil
	}

	now := time.Now()
	// If the duration is less than one second, we must force the
	// value to 1 to avoid accidentally making it never expire.
	if dura < 1*time.Second {
		return timestamppb.New(now.Add(1 * time.Second))
	}

	if dura < 30*24*time.Hour {
		return timestamppb.New(now.Add(dura))
	}

	// Send the duration as a unix timestamp of now plus duration.
	return timestamppb.New(time.Now().Add(dura))
}
