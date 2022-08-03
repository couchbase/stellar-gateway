package gocbps

import (
	"context"

	data_v1 "github.com/couchbase/stellar-nebula/genproto/data/v1"
)

type LookupInOperation uint32

const (
	LookupInOperationGet LookupInOperation = iota + 1
	LookupInOperationExists
	LookupInOperationCount
)

func (op LookupInOperation) toProto() data_v1.LookupInRequest_Spec_Operation {
	switch op {
	case LookupInOperationGet:
		return data_v1.LookupInRequest_Spec_GET
	case LookupInOperationExists:
		return data_v1.LookupInRequest_Spec_EXISTS
	case LookupInOperationCount:
		return data_v1.LookupInRequest_Spec_COUNT
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

	req := &data_v1.LookupInRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	}

	if opts.AccessDeleted {
		req.Flags = &data_v1.LookupInRequest_Flags{
			AccessDeleted: &opts.AccessDeleted,
		}
	}

	reqSpecs := make([]*data_v1.LookupInRequest_Spec, len(specs))
	for i, spec := range specs {
		reqSpec := &data_v1.LookupInRequest_Spec{
			Path:      spec.Path,
			Operation: spec.Operation.toProto(),
		}
		if spec.IsXattr {
			reqSpec.Flags = &data_v1.LookupInRequest_Spec_Flags{
				Xattr: &spec.IsXattr,
			}
		}

		reqSpecs[i] = reqSpec
	}
	req.Specs = reqSpecs

	resp, err := client.dataClient.LookupIn(ctx, req)
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

func (op MutateInOperation) toProto() data_v1.MutateInRequest_Spec_Operation {
	switch op {
	case MutateInOperationInsert:
		return data_v1.MutateInRequest_Spec_INSERT
	case MutateInOperationUpsert:
		return data_v1.MutateInRequest_Spec_UPSERT
	case MutateInOperationReplace:
		return data_v1.MutateInRequest_Spec_REPLACE
	case MutateInOperationRemove:
		return data_v1.MutateInRequest_Spec_REMOVE
	case MutateInOperationArrayAppend:
		return data_v1.MutateInRequest_Spec_ARRAY_APPEND
	case MutateInOperationArrayPrepend:
		return data_v1.MutateInRequest_Spec_ARRAY_PREPEND
	case MutateInOperationArrayInsert:
		return data_v1.MutateInRequest_Spec_ARRAY_INSERT
	case MutateInOperationArrayAddUnique:
		return data_v1.MutateInRequest_Spec_ARRAY_ADD_UNIQUE
	case MutateInOperationCounter:
		return data_v1.MutateInRequest_Spec_COUNTER
	}

	return 0
}

type StoreSemantic uint32

const (
	StoreSemanticReplace StoreSemantic = iota + 1
	StoreSemanticUpsert
	StoreSemanticInsert
)

func (s StoreSemantic) toProto() *data_v1.MutateInRequest_StoreSemantic {
	var semantic data_v1.MutateInRequest_StoreSemantic
	switch s {
	case StoreSemanticReplace:
		semantic = data_v1.MutateInRequest_REPLACE
	case StoreSemanticUpsert:
		semantic = data_v1.MutateInRequest_UPSERT
	case StoreSemanticInsert:
		semantic = data_v1.MutateInRequest_INSERT
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

	req := &data_v1.MutateInRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collName,
		Key:            id,
	}

	if opts.DurabilityLevel != DurabilityLevelUnknown {
		req.DurabilitySpec = &data_v1.MutateInRequest_DurabilityLevel{
			DurabilityLevel: *opts.DurabilityLevel.toProto(),
		}
	}
	if opts.ReplicateTo > 0 || opts.PersistTo > 0 {
		req.DurabilitySpec = &data_v1.MutateInRequest_LegacyDurabilitySpec{
			LegacyDurabilitySpec: &data_v1.LegacyDurabilitySpec{
				NumPersisted:  opts.PersistTo,
				NumReplicated: opts.ReplicateTo,
			},
		}
	}
	if opts.StoreSemantic > 0 {
		req.StoreSemantic = opts.StoreSemantic.toProto()
	}

	if opts.AccessDeleted {
		req.Flags = &data_v1.MutateInRequest_Flags{
			AccessDeleted: &opts.AccessDeleted,
		}
	}

	reqSpecs := make([]*data_v1.MutateInRequest_Spec, len(specs))
	for i, spec := range specs {
		reqSpec := &data_v1.MutateInRequest_Spec{
			Path:      spec.Path,
			Operation: spec.Operation.toProto(),
			Content:   spec.Content,
		}
		if spec.IsXattr {
			reqSpec.Flags = &data_v1.MutateInRequest_Spec_Flags{
				Xattr: &spec.IsXattr,
			}
		}

		reqSpecs[i] = reqSpec
	}
	req.Specs = reqSpecs

	resp, err := client.dataClient.MutateIn(ctx, req)
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
		Cas:           Cas(resp.Cas.Value),
		MutationToken: mutationTokenFromPs(resp.MutationToken),
	}, nil
}
