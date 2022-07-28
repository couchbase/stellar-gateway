package gocbps

import (
	"context"
	"github.com/couchbase/stellar-nebula/protos"
)

type LookupInOperation uint32

const (
	LookupInOperationGet LookupInOperation = iota + 1
	LookupInOperationExists
	LookupInOperationCount
)

func (op LookupInOperation) toProto() protos.LookupInRequest_Spec_Operation {
	switch op {
	case LookupInOperationGet:
		return protos.LookupInRequest_Spec_GET
	case LookupInOperationExists:
		return protos.LookupInRequest_Spec_EXISTS
	case LookupInOperationCount:
		return protos.LookupInRequest_Spec_COUNT
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

	req := &protos.LookupInRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	}

	if opts.AccessDeleted {
		req.Flags = &protos.LookupInRequest_Flags{
			AccessDeleted: &opts.AccessDeleted,
		}
	}

	reqSpecs := make([]*protos.LookupInRequest_Spec, len(specs))
	for i, spec := range specs {
		reqSpec := &protos.LookupInRequest_Spec{
			Path:      spec.Path,
			Operation: spec.Operation.toProto(),
		}
		if spec.IsXattr {
			reqSpec.Flags = &protos.LookupInRequest_Spec_Flags{
				Xattr: &spec.IsXattr,
			}
		}

		reqSpecs[i] = reqSpec
	}
	req.Specs = reqSpecs

	resp, err := client.couchbaseClient.LookupIn(ctx, req)
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
		Cas:     Cas(resp.Cas.Value),
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

func (op MutateInOperation) toProto() protos.MutateInRequest_Spec_Operation {
	switch op {
	case MutateInOperationInsert:
		return protos.MutateInRequest_Spec_INSERT
	case MutateInOperationUpsert:
		return protos.MutateInRequest_Spec_UPSERT
	case MutateInOperationReplace:
		return protos.MutateInRequest_Spec_REPLACE
	case MutateInOperationRemove:
		return protos.MutateInRequest_Spec_REMOVE
	case MutateInOperationArrayAppend:
		return protos.MutateInRequest_Spec_ARRAY_APPEND
	case MutateInOperationArrayPrepend:
		return protos.MutateInRequest_Spec_ARRAY_PREPEND
	case MutateInOperationArrayInsert:
		return protos.MutateInRequest_Spec_ARRAY_INSERT
	case MutateInOperationArrayAddUnique:
		return protos.MutateInRequest_Spec_ARRAY_ADD_UNIQUE
	case MutateInOperationCounter:
		return protos.MutateInRequest_Spec_COUNTER
	}

	return 0
}

type StoreSemantic uint32

const (
	StoreSemanticReplace StoreSemantic = iota + 1
	StoreSemanticUpsert
	StoreSemanticInsert
)

func (s StoreSemantic) toProto() *protos.MutateInRequest_StoreSemantic {
	var semantic protos.MutateInRequest_StoreSemantic
	switch s {
	case StoreSemanticReplace:
		semantic = protos.MutateInRequest_REPLACE
	case StoreSemanticUpsert:
		semantic = protos.MutateInRequest_UPSERT
	case StoreSemanticInsert:
		semantic = protos.MutateInRequest_INSERT
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
	Cas     Cas
	Content []MutateInResultSpec
}

func (c *Collection) MutateIn(ctx context.Context, id string, specs []MutateInSpec, opts *MutateInOptions) (*MutateInResult, error) {
	if opts == nil {
		opts = &MutateInOptions{}
	}
	client, bucketName, scopeName, collName := c.getClient()

	req := &protos.MutateInRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &protos.MutateInRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &protos.MutateInRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &protos.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}
	if opts.StoreSemantic > 0 {
		req.StoreSemantic = opts.StoreSemantic.toProto()
	}

	if opts.AccessDeleted {
		req.Flags = &protos.MutateInRequest_Flags{
			AccessDeleted: &opts.AccessDeleted,
		}
	}

	reqSpecs := make([]*protos.MutateInRequest_Spec, len(specs))
	for i, spec := range specs {
		reqSpec := &protos.MutateInRequest_Spec{
			Path:      spec.Path,
			Operation: spec.Operation.toProto(),
			Content:   spec.Content,
		}
		if spec.IsXattr {
			reqSpec.Flags = &protos.MutateInRequest_Spec_Flags{
				Xattr: &spec.IsXattr,
			}
		}

		reqSpecs[i] = reqSpec
	}
	req.Specs = reqSpecs

	resp, err := client.couchbaseClient.MutateIn(ctx, req)
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
		Content: resSpecs,
		Cas:     Cas(resp.Cas.Value),
	}, nil
}
