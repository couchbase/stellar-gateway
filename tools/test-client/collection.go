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
	/*
		if opts == nil {
			opts = &GetOptions{}
		}
	*/
	client, bucketName, scopeName, collName := c.getClient()

	req := &kv_v1.GetRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	}

	resp, err := client.kvClient.Get(ctx, req)
	if err != nil {
		return nil, err
	}

	return &GetResult{
		Content: resp.GetContentUncompressed(),
		Cas:     Cas(resp.Cas),
	}, nil
}

type ExistsOptions struct {
}

type ExistsResult struct {
	Exists bool
	Cas    Cas
}

func (c *Collection) Exists(ctx context.Context, id string, opts *ExistsOptions) (*ExistsResult, error) {
	/*
		if opts == nil {
			opts = &ExistsOptions{}
		}
	*/
	client, bucketName, scopeName, collName := c.getClient()

	req := &kv_v1.ExistsRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	}

	resp, err := client.kvClient.Exists(ctx, req)
	if err != nil {
		return nil, err
	}

	return &ExistsResult{
		Exists: resp.Result,
		Cas:    Cas(resp.Cas),
	}, nil
}

type GetAndTouchOptions struct {
}

func (c *Collection) GetAndTouch(ctx context.Context, id string, expiry time.Duration, opts *GetAndTouchOptions) (*GetResult, error) {
	/*
		if opts == nil {
			opts = &GetAndTouchOptions{}
		}
	*/
	client, bucketName, scopeName, collName := c.getClient()

	req := &kv_v1.GetAndTouchRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry: &kv_v1.GetAndTouchRequest_ExpiryTime{
			ExpiryTime: durationToTimestamp(expiry),
		},
	}

	resp, err := client.kvClient.GetAndTouch(ctx, req)
	if err != nil {
		return nil, err
	}

	return &GetResult{
		Content: resp.GetContentUncompressed(),
		Cas:     Cas(resp.Cas),
	}, nil
}

type GetAndLockOptions struct {
}

func (c *Collection) GetAndLock(ctx context.Context, id string, lockTime time.Duration, opts *GetAndLockOptions) (*GetResult, error) {
	/*
		if opts == nil {
			opts = &GetAndLockOptions{}
		}
	*/
	client, bucketName, scopeName, collName := c.getClient()

	req := &kv_v1.GetAndLockRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		LockTime:       uint32(lockTime / time.Second),
	}

	resp, err := client.kvClient.GetAndLock(ctx, req)
	if err != nil {
		return nil, err
	}

	return &GetResult{
		Content: resp.GetContentUncompressed(),
		Cas:     Cas(resp.Cas),
	}, nil
}

type UnlockOptions struct {
}

func (c *Collection) Unlock(ctx context.Context, id string, cas Cas, opts *UnlockOptions) error {
	/*
		if opts == nil {
			opts = &UnlockOptions{}
		}
	*/
	client, bucketName, scopeName, collName := c.getClient()

	req := &kv_v1.UnlockRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Cas:            uint64(cas),
	}

	_, err := client.kvClient.Unlock(ctx, req)
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

	req := &kv_v1.UpsertRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content: &kv_v1.UpsertRequest_ContentUncompressed{
			ContentUncompressed: content,
		},
		ContentFlags: 0,
		Expiry: &kv_v1.UpsertRequest_ExpiryTime{
			ExpiryTime: durationToTimestamp(opts.Expiry),
		},
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilityLevel = opts.DurabilityLevel.toProto()
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		return nil, errors.New("legacy durability is not supported")
	}

	resp, err := client.kvClient.Upsert(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas:           Cas(resp.Cas),
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

	req := &kv_v1.InsertRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content: &kv_v1.InsertRequest_ContentUncompressed{
			ContentUncompressed: content,
		},
		ContentFlags: 0,
		Expiry: &kv_v1.InsertRequest_ExpiryTime{
			ExpiryTime: durationToTimestamp(opts.Expiry),
		},
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilityLevel = opts.DurabilityLevel.toProto()
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		return nil, errors.New("legacy durability is not supported")
	}

	resp, err := client.kvClient.Insert(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas:           Cas(resp.Cas),
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

	var cas *uint64
	if opts.Cas > 0 {
		protoCas := uint64(opts.Cas)
		cas = &protoCas
	}

	req := &kv_v1.ReplaceRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Content: &kv_v1.ReplaceRequest_ContentUncompressed{
			ContentUncompressed: content,
		},
		ContentFlags: 0,
		Expiry: &kv_v1.ReplaceRequest_ExpiryTime{
			ExpiryTime: durationToTimestamp(opts.Expiry),
		},
		Cas: cas,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilityLevel = opts.DurabilityLevel.toProto()
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		return nil, errors.New("legacy durability is not supported")
	}

	resp, err := client.kvClient.Replace(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas:           Cas(resp.Cas),
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

	var cas *uint64
	if opts.Cas > 0 {
		protoCas := uint64(opts.Cas)
		cas = &protoCas
	}

	req := &kv_v1.RemoveRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Cas:            cas,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilityLevel = opts.DurabilityLevel.toProto()
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		return nil, errors.New("legacy durability is not supported")
	}

	resp, err := client.kvClient.Remove(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas:           Cas(resp.Cas),
		MutationToken: mutationTokenFromPs(resp.MutationToken),
	}, nil
}

type TouchOptions struct {
}

func (c *Collection) Touch(ctx context.Context, id string, expiry time.Duration, opts *TouchOptions) (*MutationResult, error) {
	/*
		if opts == nil {
			opts = &TouchOptions{}
		}
	*/
	client, bucketName, scopeName, collName := c.getClient()

	req := &kv_v1.TouchRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
		Expiry: &kv_v1.TouchRequest_ExpiryTime{
			ExpiryTime: durationToTimestamp(expiry),
		},
	}

	resp, err := client.kvClient.Touch(ctx, req)
	if err != nil {
		return nil, err
	}

	return &MutationResult{
		Cas:           Cas(resp.Cas),
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
