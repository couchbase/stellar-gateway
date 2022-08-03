package gocbps

import (
	"context"
	"time"

	data_v1 "github.com/couchbase/stellar-nebula/genproto/data/v1"
	couchbase_v1 "github.com/couchbase/stellar-nebula/genproto/v1"
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

	req := &data_v1.GetRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	}

	resp, err := client.dataClient.Get(ctx, req)
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

	req := &data_v1.ExistsRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	}

	resp, err := client.dataClient.Exists(ctx, req)
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

	req := &data_v1.GetAndTouchRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry:         durationToTimestamp(expiry),
	}

	resp, err := client.dataClient.GetAndTouch(ctx, req)
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

	req := &data_v1.GetAndLockRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		LockTime:       uint32(lockTime / time.Second),
	}

	resp, err := client.dataClient.GetAndLock(ctx, req)
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

	req := &data_v1.UnlockRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Cas:            cas.toProto(),
	}

	_, err := client.dataClient.Unlock(ctx, req)
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
	Cas           Cas
	MutationToken *MutationToken
}

func (c *Collection) Upsert(ctx context.Context, id string, content []byte, opts *UpsertOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &UpsertOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	req := &data_v1.UpsertRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content:        content,
		ContentType:    data_v1.DocumentContentType_JSON,
		Expiry:         durationToTimestamp(opts.Expiry),
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &data_v1.UpsertRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &data_v1.UpsertRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &data_v1.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}
	resp, err := client.dataClient.Upsert(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas:           Cas(resp.Cas.Value),
		MutationToken: mutationTokenFromPs(resp.MutationToken),
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

	req := &data_v1.InsertRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content:        content,
		ContentType:    data_v1.DocumentContentType_JSON,
		Expiry:         durationToTimestamp(opts.Expiry),
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &data_v1.InsertRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &data_v1.InsertRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &data_v1.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}

	resp, err := client.dataClient.Insert(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas:           Cas(resp.Cas.Value),
		MutationToken: mutationTokenFromPs(resp.MutationToken),
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

	var cas *couchbase_v1.Cas
	if opts.Cas > 0 {
		cas = &couchbase_v1.Cas{
			Value: uint64(opts.Cas),
		}
	}

	req := &data_v1.ReplaceRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content:        content,
		ContentType:    data_v1.DocumentContentType_JSON,
		Expiry:         durationToTimestamp(opts.Expiry),
		Cas:            cas,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &data_v1.ReplaceRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &data_v1.ReplaceRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &data_v1.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}

	resp, err := client.dataClient.Replace(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas:           Cas(resp.Cas.Value),
		MutationToken: mutationTokenFromPs(resp.MutationToken),
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

	var cas *couchbase_v1.Cas
	if opts.Cas > 0 {
		cas = &couchbase_v1.Cas{
			Value: uint64(opts.Cas),
		}
	}

	req := &data_v1.RemoveRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Cas:            cas,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &data_v1.RemoveRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &data_v1.RemoveRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &data_v1.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}

	resp, err := client.dataClient.Remove(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas:           Cas(resp.Cas.Value),
		MutationToken: mutationTokenFromPs(resp.MutationToken),
	}, nil
}

type TouchOptions struct {
}

func (c *Collection) Touch(ctx context.Context, id string, expiry time.Duration, opts *TouchOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &TouchOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	req := &data_v1.TouchRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry:         durationToTimestamp(expiry),
	}

	resp, err := client.dataClient.Touch(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas:           Cas(resp.Cas.Value),
		MutationToken: mutationTokenFromPs(resp.MutationToken),
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
