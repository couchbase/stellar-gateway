package gocbps

import (
	"context"
	"time"

	"github.com/couchbase/stellar-nebula/genproto/data_v1"
)

type CollectionBinary struct {
	collection *Collection
}

func (c *Collection) Binary() *CollectionBinary {
	return &CollectionBinary{
		collection: c,
	}
}

type AppendOptions struct {
	DurabilityLevel DurabilityLevel
	PersistTo       uint32
	ReplicateTo     uint32
	Expiry          time.Duration
	Cas             Cas
}

func (c *CollectionBinary) Append(ctx context.Context, id string, content []byte, opts *AppendOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &AppendOptions{}
	}
	client, bucketName, scopeName, collName := c.collection.getClient()

	var cas *uint64
	if opts.Cas > 0 {
		protoCas := uint64(opts.Cas)
		cas = &protoCas
	}

	req := &data_v1.AppendRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content:        content,
		Cas:            cas,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &data_v1.AppendRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &data_v1.AppendRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &data_v1.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}

	resp, err := client.dataClient.Append(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas:           Cas(resp.Cas),
		MutationToken: mutationTokenFromPs(resp.MutationToken),
	}, nil
}

type PrependOptions struct {
	DurabilityLevel DurabilityLevel
	PersistTo       uint32
	ReplicateTo     uint32
	Expiry          time.Duration
	Cas             Cas
}

func (c *CollectionBinary) Prepend(ctx context.Context, id string, content []byte, opts *PrependOptions) (*MutationResult, error) {
	if opts == nil {
		opts = &PrependOptions{}
	}
	client, bucketName, scopeName, collName := c.collection.getClient()

	var cas *uint64
	if opts.Cas > 0 {
		protoCas := uint64(opts.Cas)
		cas = &protoCas
	}

	req := &data_v1.PrependRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content:        content,
		Cas:            cas,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &data_v1.PrependRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &data_v1.PrependRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &data_v1.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}

	resp, err := client.dataClient.Prepend(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas:           Cas(resp.Cas),
		MutationToken: mutationTokenFromPs(resp.MutationToken),
	}, nil
}

type CounterResult struct {
	Cas           Cas
	Content       int64
	MutationToken *MutationToken
}

type IncrementOptions struct {
	DurabilityLevel DurabilityLevel
	PersistTo       uint32
	ReplicateTo     uint32
	Expiry          time.Duration

	Initial *int64
	Delta   uint64
}

func (c *CollectionBinary) Increment(ctx context.Context, id string, opts *IncrementOptions) (*CounterResult, error) {
	if opts == nil {
		opts = &IncrementOptions{}
	}
	client, bucketName, scopeName, collName := c.collection.getClient()

	req := &data_v1.IncrementRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry:         durationToTimestamp(opts.Expiry),
		Delta:          opts.Delta,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &data_v1.IncrementRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &data_v1.IncrementRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &data_v1.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}
	if opts.Initial != nil {
		req.Initial = opts.Initial
	}

	resp, err := client.dataClient.Increment(ctx, req)
	if err != nil {
		return nil, err
	}

	return &CounterResult{
		Cas:           Cas(resp.Cas),
		Content:       resp.Content,
		MutationToken: mutationTokenFromPs(resp.MutationToken),
	}, nil
}

type DecrementOptions struct {
	DurabilityLevel DurabilityLevel
	PersistTo       uint32
	ReplicateTo     uint32
	Expiry          time.Duration

	Initial *int64
	Delta   uint64
}

func (c *CollectionBinary) Decrement(ctx context.Context, id string, opts *DecrementOptions) (*CounterResult, error) {
	if opts == nil {
		opts = &DecrementOptions{}
	}
	client, bucketName, scopeName, collName := c.collection.getClient()

	req := &data_v1.DecrementRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry:         durationToTimestamp(opts.Expiry),
		Delta:          opts.Delta,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &data_v1.DecrementRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &data_v1.DecrementRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &data_v1.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}
	if opts.Initial != nil {
		req.Initial = opts.Initial
	}

	resp, err := client.dataClient.Decrement(ctx, req)
	if err != nil {
		return nil, err
	}

	return &CounterResult{
		Cas:           Cas(resp.Cas),
		Content:       resp.Content,
		MutationToken: mutationTokenFromPs(resp.MutationToken),
	}, nil
}
