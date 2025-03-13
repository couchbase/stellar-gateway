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

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
)

type LookupInOperation uint32

const (
	LookupInOperationGet LookupInOperation = iota + 1
	LookupInOperationExists
	LookupInOperationCount
)

func (op LookupInOperation) toProto() kv_v1.LookupInRequest_Spec_Operation {
	switch op {
	case LookupInOperationGet:
		return kv_v1.LookupInRequest_Spec_OPERATION_GET
	case LookupInOperationExists:
		return kv_v1.LookupInRequest_Spec_OPERATION_EXISTS
	case LookupInOperationCount:
		return kv_v1.LookupInRequest_Spec_OPERATION_COUNT
	}

	return 0
}

type LookupInOptions struct {
	AccessDeleted bool
}

type LookupInSpec struct {
	Path      string
	Operation LookupInOperation
	IsXattr   bool
}

type LookupInResultSpec struct {
	Content []byte
}

type LookupInResult struct {
	Cas     Cas
	Content []LookupInResultSpec
}

func (c *Collection) LookupIn(ctx context.Context, id string, specs []LookupInSpec, opts *LookupInOptions) (*LookupInResult, error) {
	if opts == nil {
		opts = &LookupInOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	req := &kv_v1.LookupInRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	}

	if opts.AccessDeleted {
		req.Flags = &kv_v1.LookupInRequest_Flags{
			AccessDeleted: &opts.AccessDeleted,
		}
	}

	reqSpecs := make([]*kv_v1.LookupInRequest_Spec, len(specs))
	for i, spec := range specs {
		reqSpec := &kv_v1.LookupInRequest_Spec{
			Path:      spec.Path,
			Operation: spec.Operation.toProto(),
		}
		if spec.IsXattr {
			reqSpec.Flags = &kv_v1.LookupInRequest_Spec_Flags{
				Xattr: &spec.IsXattr,
			}
		}

		reqSpecs[i] = reqSpec
	}
	req.Specs = reqSpecs

	resp, err := client.kvClient.LookupIn(ctx, req)
	if err != nil {
		return nil, err
	}

	var resSpecs = make([]LookupInResultSpec, len(resp.Specs))
	for i, spec := range resp.Specs {
		resSpecs[i] = LookupInResultSpec{
			Content: spec.Content,
		}
	}

	return &LookupInResult{
		Content: resSpecs,
		Cas:     Cas(resp.Cas),
	}, nil
}

type MutateInOperation uint32

const (
	MutateInOperationInsert = iota + 1
	MutateInOperationUpsert
	MutateInOperationReplace
	MutateInOperationRemove
	MutateInOperationArrayAppend
	MutateInOperationArrayPrepend
	MutateInOperationArrayInsert
	MutateInOperationArrayAddUnique
	MutateInOperationCounter
)

func (op MutateInOperation) toProto() kv_v1.MutateInRequest_Spec_Operation {
	switch op {
	case MutateInOperationInsert:
		return kv_v1.MutateInRequest_Spec_OPERATION_INSERT
	case MutateInOperationUpsert:
		return kv_v1.MutateInRequest_Spec_OPERATION_UPSERT
	case MutateInOperationReplace:
		return kv_v1.MutateInRequest_Spec_OPERATION_REPLACE
	case MutateInOperationRemove:
		return kv_v1.MutateInRequest_Spec_OPERATION_REMOVE
	case MutateInOperationArrayAppend:
		return kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_APPEND
	case MutateInOperationArrayPrepend:
		return kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_PREPEND
	case MutateInOperationArrayInsert:
		return kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_INSERT
	case MutateInOperationArrayAddUnique:
		return kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_ADD_UNIQUE
	case MutateInOperationCounter:
		return kv_v1.MutateInRequest_Spec_OPERATION_COUNTER
	}

	return 0
}

type StoreSemantic uint32

const (
	StoreSemanticReplace StoreSemantic = iota + 1
	StoreSemanticUpsert
	StoreSemanticInsert
)

func (s StoreSemantic) toProto() *kv_v1.MutateInRequest_StoreSemantic {
	var semantic kv_v1.MutateInRequest_StoreSemantic
	switch s {
	case StoreSemanticReplace:
		semantic = kv_v1.MutateInRequest_STORE_SEMANTIC_REPLACE
	case StoreSemanticUpsert:
		semantic = kv_v1.MutateInRequest_STORE_SEMANTIC_UPSERT
	case StoreSemanticInsert:
		semantic = kv_v1.MutateInRequest_STORE_SEMANTIC_INSERT
	}

	return &semantic
}

type MutateInOptions struct {
	AccessDeleted   bool
	StoreSemantic   StoreSemantic
	PersistTo       uint32
	ReplicateTo     uint32
	DurabilityLevel DurabilityLevel
}

type MutateInSpec struct {
	Path      string
	Operation MutateInOperation
	IsXattr   bool
	Content   []byte
}

type MutateInResultSpec struct {
	Content []byte
}

type MutateInResult struct {
	Cas           Cas
	Content       []MutateInResultSpec
	MutationToken *MutationToken
}

func (c *Collection) MutateIn(ctx context.Context, id string, specs []MutateInSpec, opts *MutateInOptions) (*MutateInResult, error) {
	if opts == nil {
		opts = &MutateInOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	req := &kv_v1.MutateInRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilityLevel = opts.DurabilityLevel.toProto()
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		return nil, errors.New("legacy durability is not supported")
	}
	if opts.StoreSemantic > 0 {
		req.StoreSemantic = opts.StoreSemantic.toProto()
	}
	if opts.AccessDeleted {
		req.Flags = &kv_v1.MutateInRequest_Flags{
			AccessDeleted: &opts.AccessDeleted,
		}
	}

	reqSpecs := make([]*kv_v1.MutateInRequest_Spec, len(specs))
	for i, spec := range specs {
		reqSpec := &kv_v1.MutateInRequest_Spec{
			Path:      spec.Path,
			Operation: spec.Operation.toProto(),
			Content:   spec.Content,
		}
		if spec.IsXattr {
			reqSpec.Flags = &kv_v1.MutateInRequest_Spec_Flags{
				Xattr: &spec.IsXattr,
			}
		}

		reqSpecs[i] = reqSpec
	}
	req.Specs = reqSpecs

	resp, err := client.kvClient.MutateIn(ctx, req)
	if err != nil {
		return nil, err
	}

	var resSpecs = make([]MutateInResultSpec, len(resp.Specs))
	for i, spec := range resp.Specs {
		resSpecs[i] = MutateInResultSpec{
			Content: spec.Content,
		}
	}

	return &MutateInResult{
		Content:       resSpecs,
		Cas:           Cas(resp.Cas),
		MutationToken: mutationTokenFromPs(resp.MutationToken),
	}, nil
}
