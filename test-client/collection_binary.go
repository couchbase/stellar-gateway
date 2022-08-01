package gocbps

import (
	"context"
	"github.com/couchbase/stellar-nebula/protos"
	"time"
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

	var cas *protos.Cas
	if opts.Cas > 0 {
		cas = &protos.Cas{
			Value: uint64(opts.Cas),
		}
	}

	req := &protos.AppendRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content:        content,
		Cas:            cas,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &protos.AppendRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &protos.AppendRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &protos.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}

	resp, err := client.couchbaseClient.Append(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas: Cas(resp.Cas.Value),
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

	var cas *protos.Cas
	if opts.Cas > 0 {
		cas = &protos.Cas{
			Value: uint64(opts.Cas),
		}
	}

	req := &protos.PrependRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content:        content,
		Cas:            cas,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &protos.PrependRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &protos.PrependRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &protos.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}

	resp, err := client.couchbaseClient.Prepend(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas: Cas(resp.Cas.Value),
	}, nil
}

type CounterResult struct {
	Cas     Cas
	Content int64
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

	req := &protos.IncrementRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry:         durationToTimestamp(opts.Expiry),
		Delta:          opts.Delta,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &protos.IncrementRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &protos.IncrementRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &protos.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}
	if opts.Initial != nil {
		req.Initial = opts.Initial
	}

	resp, err := client.couchbaseClient.Increment(ctx, req)
	if err != nil {
		return nil, err
	}

	return &CounterResult{
		Cas:     Cas(resp.Cas.Value),
		Content: resp.Content,
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

	req := &protos.DecrementRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry:         durationToTimestamp(opts.Expiry),
		Delta:          opts.Delta,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &protos.DecrementRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &protos.DecrementRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &protos.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}
	if opts.Initial != nil {
		req.Initial = opts.Initial
	}

	resp, err := client.couchbaseClient.Decrement(ctx, req)
	if err != nil {
		return nil, err
	}

	return &CounterResult{
		Cas:     Cas(resp.Cas.Value),
		Content: resp.Content,
	}, nil
}
