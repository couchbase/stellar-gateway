package server_v1

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/status"
)

type XdcrServer struct {
	internal_xdcr_v1.UnimplementedXdcrServiceServer

	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
}

func NewXdcrServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
) *XdcrServer {
	return &XdcrServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
	}
}

func (s *XdcrServer) GetVbucketInfo(in *internal_xdcr_v1.GetVbucketInfoRequest, out internal_xdcr_v1.XdcrService_GetVbucketInfoServer) error {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(out.Context(), in.BucketName)
	if errSt != nil {
		return errSt.Err()
	}

	numVbuckets := bucketAgent.NumVbuckets()

	for vbId := uint16(0); vbId < uint16(numVbuckets); vbId++ {
		resp, err := bucketAgent.DcpGetFailoverLog(out.Context(), &gocbcorex.DcpGetFailoverLogOptions{
			VbucketId:  vbId,
			OnBehalfOf: oboUser,
		})
		if err != nil {
			return s.errorHandler.NewGenericStatus(err).Err()
		}

		outEntries := make([]*internal_xdcr_v1.GetVbucketInfoResponse_FailoverEntry, len(resp.Entries))
		for i, entry := range resp.Entries {
			outEntries[i] = &internal_xdcr_v1.GetVbucketInfoResponse_FailoverEntry{
				VbucketUuid: entry.VbUuid,
				Seqno:       entry.SeqNo,
			}
		}

		out.Send(&internal_xdcr_v1.GetVbucketInfoResponse{
			Vbuckets: []*internal_xdcr_v1.GetVbucketInfoResponse_VbucketState{
				{
					VbucketId:   uint32(vbId),
					FailoverLog: outEntries,
				},
			},
		})
	}

	return nil
}

func (s *XdcrServer) GetMeta(ctx context.Context, in *internal_xdcr_v1.GetMetaRequest) (*internal_xdcr_v1.GetMetaResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.GetMetaOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.FetchDatatype = true

	result, err := bucketAgent.GetMeta(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoReadAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	var expiryTime time.Time
	if result.Expiry != 0 {
		expiryTime = time.Unix(int64(result.Expiry), 0)
	}

	resp := &internal_xdcr_v1.GetMetaResponse{
		Cas:          result.Cas,
		Expiry:       timeFromGo(expiryTime),
		IsDeleted:    result.IsDeleted,
		Seqno:        result.SeqNo,
		Datatype:     uint32(*result.Datatype),
		ContentFlags: result.Flags,
	}

	return resp, nil
}

func (s *XdcrServer) Insert(ctx context.Context, in *internal_xdcr_v1.InsertRequest) (*internal_xdcr_v1.InsertResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.AddWithMetaOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Flags = in.ContentFlags
	opts.Datatype = memdx.DatatypeFlagCompressed
	opts.Value = in.ContentCompressed

	if in.ExpiryTime != nil {
		opts.Expiry = timeExpiryToGocbcorex(timeToGo(in.ExpiryTime))
	}

	result, err := bucketAgent.AddWithMeta(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocExists) {
			return nil, s.errorHandler.NewDocExistsStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrValueTooLarge) {
			return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, false).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &internal_xdcr_v1.InsertResponse{
		Cas:   result.Cas,
		Seqno: result.MutationToken.SeqNo,
	}, nil
}

func (s *XdcrServer) Update(ctx context.Context, in *internal_xdcr_v1.UpdateRequest) (*internal_xdcr_v1.UpdateResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkCAS(&in.Cas)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.SetWithMetaOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.Flags = in.ContentFlags
	opts.Datatype = memdx.DatatypeFlagCompressed
	opts.Value = in.ContentCompressed
	opts.RevNo = in.Revno
	opts.Cas = in.Cas

	if in.ExpiryTime != nil {
		opts.Expiry = timeExpiryToGocbcorex(timeToGo(in.ExpiryTime))
	}

	result, err := bucketAgent.SetWithMeta(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocExists) {
			return nil, s.errorHandler.NewDocExistsStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrValueTooLarge) {
			return nil, s.errorHandler.NewValueTooLargeStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key, false).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &internal_xdcr_v1.UpdateResponse{
		Cas:   result.Cas,
		Seqno: result.MutationToken.SeqNo,
	}, nil
}

func (s *XdcrServer) Delete(ctx context.Context, in *internal_xdcr_v1.DeleteRequest) (*internal_xdcr_v1.DeleteResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkCAS(&in.Cas)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var opts gocbcorex.DeleteWithMetaOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.RevNo = in.Revno
	opts.Cas = in.Cas

	result, err := bucketAgent.DeleteWithMeta(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrCasMismatch) {
			return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocLocked) {
			return nil, s.errorHandler.NewDocLockedStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrDocNotFound) {
			return nil, s.errorHandler.NewDocMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
		} else if errors.Is(err, memdx.ErrUnknownCollectionName) {
			return nil, s.errorHandler.NewCollectionMissingStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		} else if errors.Is(err, memdx.ErrUnknownScopeName) {
			return nil, s.errorHandler.NewScopeMissingStatus(err, in.BucketName, in.ScopeName).Err()
		} else if errors.Is(err, memdx.ErrAccessError) {
			return nil, s.errorHandler.NewCollectionNoWriteAccessStatus(err, in.BucketName, in.ScopeName, in.CollectionName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &internal_xdcr_v1.DeleteResponse{
		Cas:   result.Cas,
		Seqno: result.MutationToken.SeqNo,
	}, nil
}

func (s *XdcrServer) checkKey(key string) *status.Status {
	if len(key) > 250 || len(key) < 1 {
		return s.errorHandler.NewInvalidKeyLengthStatus(key)
	}

	return nil
}

func (s *XdcrServer) checkCAS(cas *uint64) *status.Status {
	if cas != nil && *cas == 0 {
		return s.errorHandler.NewZeroCasStatus()
	}

	return nil
}
