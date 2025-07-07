package server_v1

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
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

	getOneVbucketState := func(vbId uint16) (*internal_xdcr_v1.GetVbucketInfoResponse_VbucketState, error) {
		statsParser := memdx.VbucketDetailsStatsParser{
			VbucketID: &vbId,
		}
		_, err := bucketAgent.StatsByVbucket(out.Context(), &gocbcorex.StatsByVbucketOptions{
			VbucketID:  vbId,
			GroupName:  statsParser.GroupName(),
			OnBehalfOf: oboUser,
		}, func(resp gocbcorex.StatsDataResult) {
			statsParser.HandleEntry(resp.Key, resp.Value)
		})
		if err != nil {
			return nil, err
		}

		vbStatsResp := statsParser.Vbuckets[vbId]

		flogParser := memdx.FailoverStatsParser{
			VbucketID: &vbId,
		}
		_, err = bucketAgent.StatsByVbucket(out.Context(), &gocbcorex.StatsByVbucketOptions{
			VbucketID:  vbId,
			GroupName:  flogParser.GroupName(),
			OnBehalfOf: oboUser,
		}, func(resp gocbcorex.StatsDataResult) {
			flogParser.HandleEntry(resp.Key, resp.Value)
		})
		if err != nil {
			return nil, err
		}

		vbFlogResp := flogParser.Vbuckets[vbId]

		flogEntries := make([]*internal_xdcr_v1.GetVbucketInfoResponse_FailoverEntry, len(vbFlogResp.FailoverLog))
		for i, entry := range vbFlogResp.FailoverLog {
			flogEntries[i] = &internal_xdcr_v1.GetVbucketInfoResponse_FailoverEntry{
				Uuid:  entry.VbUuid,
				Seqno: entry.SeqNo,
			}
		}

		return &internal_xdcr_v1.GetVbucketInfoResponse_VbucketState{
			VbucketId:   uint32(vbId),
			FailoverLog: flogEntries,
			HighSeqno:   vbStatsResp.HighSeqno,
			MaxCas:      vbStatsResp.MaxCas,
		}, nil
	}

	numVbuckets := bucketAgent.NumVbuckets()
	waitCh := make(chan error, numVbuckets)
	for vbId := uint16(0); vbId < uint16(numVbuckets); vbId++ {
		go func() {
			vbState, err := getOneVbucketState(vbId)
			if err != nil {
				waitCh <- err
				return
			}

			out.Send(&internal_xdcr_v1.GetVbucketInfoResponse{
				Vbuckets: []*internal_xdcr_v1.GetVbucketInfoResponse_VbucketState{vbState},
			})
			waitCh <- nil
		}()
	}

	for vbId := uint16(0); vbId < uint16(numVbuckets); vbId++ {
		err := <-waitCh
		if err != nil {
			// TODO(brett19): Handle this error better...
			s.logger.Debug("Error retrieving vbucket state",
				zap.String("bucket", in.BucketName),
				zap.Uint16("vbucket_id", vbId),
				zap.Error(err),
			)
			return s.errorHandler.NewGenericStatus(err).Err()
		}
	}

	return nil
}

func (s *XdcrServer) GetDocument(
	ctx context.Context,
	in *internal_xdcr_v1.GetDocumentRequest,
) (*internal_xdcr_v1.GetDocumentResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	resp := &internal_xdcr_v1.GetDocumentResponse{}
	var metaCas uint64
	var dataCas uint64

	for i := 0; i < 7; i++ {
		var metaOpts gocbcorex.GetMetaOptions
		metaOpts.OnBehalfOf = oboUser
		metaOpts.ScopeName = in.ScopeName
		metaOpts.CollectionName = in.CollectionName
		metaOpts.Key = []byte(in.Key)
		metaOpts.FetchDatatype = true

		metaRes, err := bucketAgent.GetMeta(ctx, &metaOpts)
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
		if metaRes.Expiry != 0 {
			expiryTime = time.Unix(int64(metaRes.Expiry), 0)
		}

		resp.Cas = metaRes.Cas
		resp.Expiry = timeFromGo(expiryTime)
		resp.IsDeleted = metaRes.IsDeleted
		resp.Revno = metaRes.RevNo
		resp.Datatype = uint32(*metaRes.Datatype)
		resp.ContentFlags = metaRes.Flags
		metaCas = metaRes.Cas

		// if the user didn't want the content, or we've already retrieved the content
		// but had to loop around due to a cas mismatch, we can return early
		if !in.IncludeContent || dataCas == metaCas {
			return resp, nil
		}

		var dataOpts gocbcorex.GetOptions
		dataOpts.OnBehalfOf = oboUser
		dataOpts.ScopeName = in.ScopeName
		dataOpts.CollectionName = in.CollectionName
		dataOpts.Key = []byte(in.Key)

		dataRes, err := bucketAgent.Get(ctx, &dataOpts)
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

		alwaysCompress := kv_v1.CompressionEnabled_COMPRESSION_ENABLED_ALWAYS
		isCompressed, respValue, errSt :=
			CompressHandler{}.MaybeCompressContent(dataRes.Value, dataRes.Datatype, &alwaysCompress)
		if errSt != nil {
			return nil, errSt.Err()
		}
		if !isCompressed {
			return nil, s.errorHandler.NewGenericStatus(
				errors.New("document content is not compressed, but compression was expected"),
			).Err()
		}

		resp.ContentCompressed = respValue
		dataCas = dataRes.Cas

		if dataCas == metaCas {
			return resp, nil
		}
	}

	return nil, s.errorHandler.NewGenericStatus(
		errors.New("failed to retrieve document after multiple attempts"),
	).Err()
}

func (s *XdcrServer) CheckDocument(
	ctx context.Context,
	in *internal_xdcr_v1.CheckDocumentRequest,
) (*internal_xdcr_v1.CheckDocumentResponse, error) {
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

	metaRes, err := bucketAgent.GetMeta(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
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

	crMode, err := bucketAgent.GetConflictResolutionMode(ctx)
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	// TODO(brett19): Move this to gocbcorex instead...
	if crMode == cbmgmtx.ConflictResolutionTypeTimestamp {
		if in.Cas < metaRes.Cas {
			return nil, s.errorHandler.NewDocConflictStatus(
				errors.New("document CAS is lower than the current CAS"),
				in.BucketName, in.ScopeName, in.CollectionName, in.Key,
			).Err()
		} else if in.Cas == metaRes.Cas {
			if in.Revno < metaRes.RevNo {
				return nil, s.errorHandler.NewDocConflictStatus(
					errors.New("document revision number is lower than the current revision"),
					in.BucketName, in.ScopeName, in.CollectionName, in.Key,
				).Err()
			} else if in.Revno == metaRes.RevNo {
				return nil, s.errorHandler.NewDocConflictStatus(
					errors.New("document revision number is equal and document CAS is equal to the current CAS"),
					in.BucketName, in.ScopeName, in.CollectionName, in.Key,
				).Err()
			}
		}
	} else if crMode == cbmgmtx.ConflictResolutionTypeSequenceNumber {
		if in.Revno < metaRes.RevNo {
			return nil, s.errorHandler.NewDocConflictStatus(
				errors.New("document revision number is lower than the current revision"),
				in.BucketName, in.ScopeName, in.CollectionName, in.Key,
			).Err()
		} else if in.Revno == metaRes.RevNo {
			if in.Cas < metaRes.Cas {
				return nil, s.errorHandler.NewDocConflictStatus(
					errors.New("document CAS is lower than the current CAS"),
					in.BucketName, in.ScopeName, in.CollectionName, in.Key,
				).Err()
			} else if in.Cas == metaRes.Cas {
				return nil, s.errorHandler.NewDocConflictStatus(
					errors.New("document CAS is equal and document revision number is equal to the current revision"),
					in.BucketName, in.ScopeName, in.CollectionName, in.Key,
				).Err()
			}
		}
	} else {
		// TODO(brett19): Handle other conflict resolution modes...
		return nil, s.errorHandler.NewGenericStatus(
			errors.New("unsupported conflict resolution mode"),
		).Err()
	}

	return &internal_xdcr_v1.CheckDocumentResponse{}, nil
}

func (s *XdcrServer) PushDocument(
	ctx context.Context,
	in *internal_xdcr_v1.PushDocumentRequest,
) (*internal_xdcr_v1.PushDocumentResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkCAS(&in.StoreCas)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var docDatatype memdx.DatatypeFlag
	if in.ContentType == internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON {
		docDatatype |= memdx.DatatypeFlagJSON
	}

	if in.CheckCas != nil && *in.CheckCas == 0 {
		var opts gocbcorex.AddWithMetaOptions
		opts.OnBehalfOf = oboUser
		opts.ScopeName = in.ScopeName
		opts.CollectionName = in.CollectionName
		opts.Key = []byte(in.Key)
		opts.Flags = in.ContentFlags
		opts.Datatype = memdx.DatatypeFlagCompressed | docDatatype
		opts.Value = in.ContentCompressed
		opts.StoreCas = in.StoreCas

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

		return &internal_xdcr_v1.PushDocumentResponse{
			Cas:   result.Cas,
			Seqno: result.MutationToken.SeqNo,
		}, nil
	} else {
		var checkCas uint64
		if in.CheckCas != nil {
			checkCas = *in.CheckCas
		} else {
			checkCas = 0
		}

		var opts gocbcorex.SetWithMetaOptions
		opts.OnBehalfOf = oboUser
		opts.ScopeName = in.ScopeName
		opts.CollectionName = in.CollectionName
		opts.Key = []byte(in.Key)
		opts.Flags = in.ContentFlags
		opts.Datatype = memdx.DatatypeFlagCompressed | docDatatype
		opts.Value = in.ContentCompressed
		opts.RevNo = in.Revno
		opts.CheckCas = checkCas
		opts.StoreCas = in.StoreCas

		if in.ExpiryTime != nil {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(in.ExpiryTime))
		}

		if checkCas != 0 {
			opts.Options |= memdx.MetaOpFlagSkipConflictResolution
		}

		result, err := bucketAgent.SetWithMeta(ctx, &opts)
		if err != nil {
			if errors.Is(err, memdx.ErrCasMismatch) {
				if checkCas == 0 {
					// CAS Mismatch with zero CAS means the conflict resolution failed
					return nil, s.errorHandler.NewDocConflictStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
				} else {
					// CAS mismatch with non-zero cas sets SkipConflictResolution, so this
					// case is a real CAS mismatch
					return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
				}
			} else if errors.Is(err, memdx.ErrDocExists) {
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

		return &internal_xdcr_v1.PushDocumentResponse{
			Cas:   result.Cas,
			Seqno: result.MutationToken.SeqNo,
		}, nil
	}
}

func (s *XdcrServer) DeleteDocument(
	ctx context.Context,
	in *internal_xdcr_v1.DeleteDocumentRequest,
) (*internal_xdcr_v1.DeleteDocumentResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(ctx, in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkKey(in.Key)
	if errSt != nil {
		return nil, errSt.Err()
	}

	errSt = s.checkCAS(&in.StoreCas)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var checkCas uint64
	if in.CheckCas != nil {
		if *in.CheckCas == 0 {
			return nil, s.errorHandler.NewZeroCasStatus().Err()
		}

		checkCas = *in.CheckCas
	}

	var opts gocbcorex.DeleteWithMetaOptions
	opts.OnBehalfOf = oboUser
	opts.ScopeName = in.ScopeName
	opts.CollectionName = in.CollectionName
	opts.Key = []byte(in.Key)
	opts.RevNo = in.Revno
	opts.CheckCas = checkCas
	opts.StoreCas = in.StoreCas

	result, err := bucketAgent.DeleteWithMeta(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrCasMismatch) {
			// TODO(brett19): Figure out if we can merge this logic into gocbcorex instead...
			if checkCas == 0 {
				// CAS Mismatch with zero CAS means the conflict resolution failed
				return nil, s.errorHandler.NewDocConflictStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
			} else {
				// CAS mismatch with non-zero cas sets SkipConflictResolution, so this
				// case is a real CAS mismatch
				return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
			}
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

	return &internal_xdcr_v1.DeleteDocumentResponse{
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
