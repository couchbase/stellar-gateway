/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package gocbps

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
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

	req := &kv_v1.AppendRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content:        content,
		Cas:            cas,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilityLevel = opts.DurabilityLevel.toProto()
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		return nil, errors.New("legacy durability is not supported")
	}

	resp, err := client.kvClient.Append(ctx, req)
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

	req := &kv_v1.PrependRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content:        content,
		Cas:            cas,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilityLevel = opts.DurabilityLevel.toProto()
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		return nil, errors.New("legacy durability is not supported")
	}

	resp, err := client.kvClient.Prepend(ctx, req)
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

	req := &kv_v1.IncrementRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry: &kv_v1.IncrementRequest_ExpiryTime{
			ExpiryTime: durationToTimestamp(opts.Expiry),
		},
		Delta: opts.Delta,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilityLevel = opts.DurabilityLevel.toProto()
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		return nil, errors.New("legacy durability is not supported")
	}
	if opts.Initial != nil {
		req.Initial = opts.Initial
	}

	resp, err := client.kvClient.Increment(ctx, req)
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

	req := &kv_v1.DecrementRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry: &kv_v1.DecrementRequest_ExpiryTime{
			ExpiryTime: durationToTimestamp(opts.Expiry),
		},
		Delta: opts.Delta,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilityLevel = opts.DurabilityLevel.toProto()
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		return nil, errors.New("legacy durability is not supported")
	}
	if opts.Initial != nil {
		req.Initial = opts.Initial
	}

	resp, err := client.kvClient.Decrement(ctx, req)
	if err != nil {
		return nil, err
	}

	return &CounterResult{
		Cas:           Cas(resp.Cas),
		Content:       resp.Content,
		MutationToken: mutationTokenFromPs(resp.MutationToken),
	}, nil
}
