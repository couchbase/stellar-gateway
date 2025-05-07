package server_v1

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbdoccrx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
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

func (s *XdcrServer) Heartbeat(ctx context.Context, in *internal_xdcr_v1.HeartbeatRequest) (*internal_xdcr_v1.HeartbeatResponse, error) {
	clusterAgent, oboUser, errSt := s.authHandler.GetHttpOboAgent(ctx, nil)
	if errSt != nil {
		return nil, errSt.Err()
	}

	err := clusterAgent.XdcrC2c(ctx, &cbmgmtx.XdcrC2cOptions{
		Payload:    in.Payload,
		OnBehalfOf: oboUser,
	})
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &internal_xdcr_v1.HeartbeatResponse{}, nil
}

func (s *XdcrServer) GetClusterInfo(ctx context.Context, in *internal_xdcr_v1.GetClusterInfoRequest) (*internal_xdcr_v1.GetClusterInfoResponse, error) {
	clusterAgent, oboUser, errSt := s.authHandler.GetHttpOboAgent(ctx, nil)
	if errSt != nil {
		return nil, errSt.Err()
	}

	clusterInfo, err := clusterAgent.GetClusterInfo(ctx, &cbmgmtx.GetClusterInfoOptions{
		OnBehalfOf: oboUser,
	})
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	return &internal_xdcr_v1.GetClusterInfoResponse{
		ClusterUuid: clusterInfo.Uuid,
	}, nil
}

func (s *XdcrServer) GetBucketInfo(ctx context.Context, in *internal_xdcr_v1.GetBucketInfoRequest) (*internal_xdcr_v1.GetBucketInfoResponse, error) {
	bucketAgent, oboUser, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return nil, errSt.Err()
	}

	bucketInfo, err := bucketAgent.GetBucket(ctx, &cbmgmtx.GetBucketOptions{
		BucketName: in.BucketName,
		OnBehalfOf: oboUser,
	})
	if err != nil {
		if errors.Is(err, cbmgmtx.ErrBucketNotFound) {
			return nil, s.errorHandler.NewBucketMissingStatus(err, in.BucketName).Err()
		}
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	numVbuckets := uint32(0)
	if bucketInfo.RawConfig.VBucketServerMap != nil {
		numVbuckets = uint32(len(bucketInfo.RawConfig.VBucketServerMap.VBucketMap))
	}

	var conflictResolutionType internal_xdcr_v1.ConflictResolutionType
	switch bucketInfo.ConflictResolutionType {
	case cbmgmtx.ConflictResolutionTypeSequenceNumber:
		conflictResolutionType = internal_xdcr_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER
	case cbmgmtx.ConflictResolutionTypeTimestamp:
		conflictResolutionType = internal_xdcr_v1.ConflictResolutionType_CONFLICT_RESOLUTION_TYPE_TIMESTAMP
	default:
		return nil, status.New(codes.InvalidArgument, "invalid conflict resolution mode encountered").Err()
	}

	return &internal_xdcr_v1.GetBucketInfoResponse{
		BucketUuid:             bucketInfo.UUID,
		NumVbuckets:            numVbuckets,
		ConflictResolutionType: conflictResolutionType,
	}, nil
}

func (s *XdcrServer) GetVbucketInfo(in *internal_xdcr_v1.GetVbucketInfoRequest, out internal_xdcr_v1.XdcrService_GetVbucketInfoServer) error {
	bucketAgent, oboUser, errSt := s.authHandler.GetMemdOboAgent(out.Context(), in.BucketName)
	if errSt != nil {
		return errSt.Err()
	}

	getOneVbucketState := func(vbId uint16) (*internal_xdcr_v1.GetVbucketInfoResponse_VbucketState, error) {
		var vbUuid uint64
		var highSeqno uint64
		var maxCas *uint64

		if in.IncludeMaxCas != nil && *in.IncludeMaxCas {
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

			vbUuid = vbStatsResp.Uuid
			highSeqno = vbStatsResp.HighSeqno
			maxCas = ptr.To(vbStatsResp.MaxCas)
		} else {
			statsParser := memdx.VbucketSeqNoStatsParser{
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

			vbUuid = vbStatsResp.Uuid
			highSeqno = vbStatsResp.HighSeqno
			maxCas = nil
		}

		var historyEntries []*internal_xdcr_v1.GetVbucketInfoResponse_HistoryEntry
		if in.IncludeHistory != nil && *in.IncludeHistory {
			flogParser := memdx.FailoverStatsParser{
				VbucketID: &vbId,
			}
			_, err := bucketAgent.StatsByVbucket(out.Context(), &gocbcorex.StatsByVbucketOptions{
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

			historyEntries = make([]*internal_xdcr_v1.GetVbucketInfoResponse_HistoryEntry, len(vbFlogResp.FailoverLog))
			for i, entry := range vbFlogResp.FailoverLog {
				historyEntries[i] = &internal_xdcr_v1.GetVbucketInfoResponse_HistoryEntry{
					Uuid:  entry.VbUuid,
					Seqno: entry.SeqNo,
				}
			}
		}

		return &internal_xdcr_v1.GetVbucketInfoResponse_VbucketState{
			VbucketId: uint32(vbId),
			Uuid:      vbUuid,
			History:   historyEntries,
			HighSeqno: highSeqno,
			MaxCas:    maxCas,
		}, nil
	}

	numBucketVbuckets := bucketAgent.NumVbuckets()

	// convert the uint32 vbucket ids to uint16, and validate they are in range
	fetchVbs := make([]uint16, 0, numBucketVbuckets)
	for _, vbId := range in.VbucketIds {
		if vbId >= uint32(numBucketVbuckets) {
			return status.New(codes.InvalidArgument,
				fmt.Sprintf("vbucket id must be between 0 and %d", numBucketVbuckets)).Err()
		}

		fetchVbs = append(fetchVbs, uint16(vbId))
	}

	// if the user did not specify any vbuckets, we return them all
	if len(fetchVbs) == 0 {
		for vbId := uint16(0); vbId < uint16(numBucketVbuckets); vbId++ {
			fetchVbs = append(fetchVbs, vbId)
		}
	}

	waitCh := make(chan error, len(fetchVbs))
	for _, vbId := range fetchVbs {
		go func(vbId uint16) {
			vbState, err := getOneVbucketState(vbId)
			if err != nil {
				s.logger.Debug("Error retrieving vbucket state",
					zap.String("bucket", in.BucketName),
					zap.Uint16("vbucket_id", vbId),
					zap.Error(err),
				)

				waitCh <- err
				return
			}

			// we ignore send errors here as there isn't much point in trying to cancel
			// the running operation anyways, so we just let them all fail.
			_ = out.Send(&internal_xdcr_v1.GetVbucketInfoResponse{
				Vbuckets: []*internal_xdcr_v1.GetVbucketInfoResponse_VbucketState{vbState},
			})
			waitCh <- nil
		}(vbId)
	}

	for range fetchVbs {
		err := <-waitCh
		if err != nil {
			return s.errorHandler.NewGenericStatus(err).Err()
		}
	}

	return nil
}

func (s *XdcrServer) WatchCollections(in *internal_xdcr_v1.WatchCollectionsRequest, out internal_xdcr_v1.XdcrService_WatchCollectionsServer) error {
	ctx := out.Context()
	bucketAgent, oboUser, errSt := s.authHandler.GetHttpOboAgent(ctx, &in.BucketName)
	if errSt != nil {
		return errSt.Err()
	}

	var latestManifestUid uint64 = 0

	for {
		manifest, err := bucketAgent.GetCollectionManifest(out.Context(), &cbmgmtx.GetCollectionManifestOptions{
			BucketName: in.BucketName,
			OnBehalfOf: oboUser,
		})
		if err != nil {
			return s.errorHandler.NewGenericStatus(err).Err()
		}

		manifestUid, _ := strconv.ParseUint(manifest.UID, 16, 32)
		if manifestUid <= latestManifestUid {
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		latestManifestUid = manifestUid

		resp := &internal_xdcr_v1.WatchCollectionsResponse{
			ManifestUid: uint32(manifestUid),
		}
		for _, scope := range manifest.Scopes {
			scopeId, _ := strconv.ParseUint(scope.UID, 16, 32)

			scopeOut := &internal_xdcr_v1.WatchCollectionsResponse_Scope{
				ScopeId:   uint32(scopeId),
				ScopeName: scope.Name,
			}

			for _, collection := range scope.Collections {
				collectionId, _ := strconv.ParseUint(collection.UID, 16, 32)

				collectionOut := &internal_xdcr_v1.WatchCollectionsResponse_Collection{
					CollectionId:   uint32(collectionId),
					CollectionName: collection.Name,
				}

				scopeOut.Collections = append(scopeOut.Collections, collectionOut)
			}

			resp.Scopes = append(resp.Scopes, scopeOut)
		}

		err = out.Send(resp)
		if err != nil {
			return err
		}
	}
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

	if !in.IncludeContent && (in.IncludeXattrs != nil && *in.IncludeXattrs) {
		return nil, status.New(codes.InvalidArgument, "IncludeXattrs cannot be true when IncludeContent is false").Err()
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

		if in.IncludeXattrs == nil || !*in.IncludeXattrs {
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
		} else {
			var dataOpts gocbcorex.GetExOptions
			dataOpts.OnBehalfOf = oboUser
			dataOpts.ScopeName = in.ScopeName
			dataOpts.CollectionName = in.CollectionName
			dataOpts.Key = []byte(in.Key)

			dataRes, err := bucketAgent.GetEx(ctx, &dataOpts)
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
				} else if errors.Is(err, memdx.ErrUnknownCommand) {
					return nil, s.errorHandler.NewUnimplementedServerVersionStatus().Err()
				}
				return nil, s.errorHandler.NewGenericStatus(err).Err()
			}

			decompValue, errSt := CompressHandler{}.UncompressContent(dataRes.Value, dataRes.Datatype)
			if errSt != nil {
				return nil, errSt.Err()
			}

			xattrBlob, docValue, err := memdx.SplitXattrBlob(decompValue)
			if err != nil {
				return nil, s.errorHandler.NewGenericStatus(
					errors.New("failed to parse xattr data"),
				).Err()
			}

			err = memdx.IterXattrBlobEntries(xattrBlob, func(key, value string) {
				if resp.Xattrs == nil {
					resp.Xattrs = make(map[string][]byte)
				}

				resp.Xattrs[key] = []byte(value)
			})
			if err != nil {
				return nil, s.errorHandler.NewGenericStatus(
					errors.New("failed to iterate xattr data"),
				).Err()
			}

			alwaysCompress := kv_v1.CompressionEnabled_COMPRESSION_ENABLED_ALWAYS
			isCompressed, respValue, errSt :=
				CompressHandler{}.MaybeCompressContent(docValue, 0, &alwaysCompress)
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
		}

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
	opts.FetchDatatype = true

	metaRes, err := bucketAgent.GetMeta(ctx, &opts)
	if err != nil {
		if errors.Is(err, memdx.ErrDocNotFound) {
			// the document not existing means that there is nothing to conflict with,
			// and the write should succeed as long as this is not an attempt to delete.
			if !in.IsDeleted {
				return &internal_xdcr_v1.CheckDocumentResponse{}, nil
			}

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

	var inExpiry uint32
	if in.ExpiryTime != nil {
		inExpiry = timeExpiryToGocbcorex(timeToGo(in.ExpiryTime))
	}

	crRes, err := cbdoccrx.ConflictResolver{
		Mode: cbdoccrx.ConflictResolutionMode(crMode),
	}.Resolve(&cbdoccrx.Document{
		Cas:       in.StoreCas,
		RevNo:     in.Revno,
		Expiry:    inExpiry,
		Flags:     in.ContentFlags,
		IsDeleted: in.IsDeleted,
		HasXattrs: in.HasXattrs,
	}, &cbdoccrx.Document{
		Cas:       metaRes.Cas,
		RevNo:     metaRes.RevNo,
		Expiry:    metaRes.Expiry,
		Flags:     metaRes.Flags,
		IsDeleted: metaRes.IsDeleted,
		HasXattrs: metaRes.Datatype != nil && *metaRes.Datatype&memdx.DatatypeFlagXattrs != 0,
	})
	if err != nil {
		if errors.Is(err, cbdoccrx.ErrUnsupportedConflictResolutionMode) {
			return nil, s.errorHandler.NewGenericStatus(
				errors.New("unsupported conflict resolution mode"),
			).Err()
		}

		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	if crRes == cbdoccrx.ResolveResultKeepB {
		// keep b means to not replicate
		// equality means to not replicate (ie keep the target as-is)
		return nil, s.errorHandler.NewDocConflictStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
	}

	// if there is a true conflict, we need to return an error indicating that
	// the document will be overwritten and include the original document content.

	// keep the source
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
	var docContent []byte
	if !in.IsDeleted {
		if in.ContentType == internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON {
			docDatatype |= memdx.DatatypeFlagJSON
		}

		switch content := in.Content.(type) {
		case *internal_xdcr_v1.PushDocumentRequest_ContentUncompressed:
			docContent = content.ContentUncompressed
		case *internal_xdcr_v1.PushDocumentRequest_ContentCompressed:
			docContent = content.ContentCompressed
			docDatatype |= memdx.DatatypeFlagCompressed
		default:
			return nil, status.New(codes.InvalidArgument, "CompressedContent or UncompressedContent must be specified.").Err()
		}

		var xattrBlob []byte
		if in.Xattrs != nil {
			for key, value := range in.Xattrs {
				xattrBlob = memdx.AppendXattrBlobEntry(xattrBlob, key, string(value))
			}
		}
		if len(xattrBlob) > 0 {
			// if we need to add xattrs, we need to uncompress the document content first
			uncompressedContent, errSt := CompressHandler{}.UncompressContent(docContent, docDatatype)
			if errSt != nil {
				return nil, errSt.Err()
			}

			// join the xattr blob and the document content
			joinedContent := memdx.JoinXattrBlob(xattrBlob, uncompressedContent)

			// now update the document content, and mark it as include xattrs
			docContent = joinedContent
			docDatatype &^= memdx.DatatypeFlagCompressed
			docDatatype |= memdx.DatatypeFlagXattrs
		}
	} else {
		if in.Content != nil {
			return nil, status.New(codes.InvalidArgument, "Content cannot be specified when IsDeleted is true.").Err()
		}
		if in.Xattrs != nil {
			return nil, status.New(codes.InvalidArgument, "Xattrs cannot be specified when IsDeleted is true.").Err()
		}
	}

	crMode, err := bucketAgent.GetConflictResolutionMode(ctx)
	if err != nil {
		return nil, s.errorHandler.NewGenericStatus(err).Err()
	}

	if in.IsDeleted {
		var checkCas uint64
		if in.CheckCas != nil {
			if *in.CheckCas == 0 {
				return nil, s.errorHandler.NewZeroCasStatus().Err()
			}
			checkCas = *in.CheckCas
		} else {
			checkCas = 0
		}

		var opts gocbcorex.DeleteWithMetaOptions
		opts.OnBehalfOf = oboUser
		opts.ScopeName = in.ScopeName
		opts.CollectionName = in.CollectionName
		opts.Key = []byte(in.Key)
		opts.RevNo = in.Revno
		opts.CheckCas = checkCas
		opts.StoreCas = in.StoreCas

		if checkCas != 0 {
			opts.Options |= memdx.MetaOpFlagSkipConflictResolution
		}
		if crMode == cbmgmtx.ConflictResolutionTypeTimestamp {
			opts.Options |= memdx.MetaOpFlagForceAcceptWithMetaOps
		}

		if in.VbUuid != nil {
			opts.VBUUID = *in.VbUuid
		}

		result, err := bucketAgent.DeleteWithMeta(ctx, &opts)
		if err != nil {
			if errors.Is(err, memdx.ErrConflictOrCasMismatch) {
				if checkCas == 0 {
					// if there is no checked cas, this must be a conflict
					return nil, s.errorHandler.NewDocConflictStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
				}

				// we skip conflict resolution for sets with a checked CAS, so this must be a CAS mismatch
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
			} else if errors.Is(err, gocbcorex.ErrVbucketUUIDMismatch) {
				return nil, s.errorHandler.NewVbUuidDivergenceStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
			}
			return nil, s.errorHandler.NewGenericStatus(err).Err()
		}

		return &internal_xdcr_v1.PushDocumentResponse{
			Cas:   result.Cas,
			Seqno: result.MutationToken.SeqNo,
		}, nil
	} else if in.CheckCas != nil && *in.CheckCas == 0 {
		var opts gocbcorex.AddWithMetaOptions
		opts.OnBehalfOf = oboUser
		opts.ScopeName = in.ScopeName
		opts.CollectionName = in.CollectionName
		opts.Key = []byte(in.Key)
		opts.Flags = in.ContentFlags
		opts.Datatype = docDatatype
		opts.Value = docContent
		opts.StoreCas = in.StoreCas

		if in.ExpiryTime != nil {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(in.ExpiryTime))
		}

		if crMode == cbmgmtx.ConflictResolutionTypeTimestamp {
			opts.Options |= memdx.MetaOpFlagForceAcceptWithMetaOps
		}

		if in.VbUuid != nil {
			opts.VBUUID = *in.VbUuid
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
			} else if errors.Is(err, gocbcorex.ErrVbucketUUIDMismatch) {
				return nil, s.errorHandler.NewVbUuidDivergenceStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
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
		opts.Datatype = docDatatype
		opts.Value = docContent
		opts.RevNo = in.Revno
		opts.CheckCas = checkCas
		opts.StoreCas = in.StoreCas

		if in.ExpiryTime != nil {
			opts.Expiry = timeExpiryToGocbcorex(timeToGo(in.ExpiryTime))
		}

		if checkCas != 0 {
			opts.Options |= memdx.MetaOpFlagSkipConflictResolution
		}
		if crMode == cbmgmtx.ConflictResolutionTypeTimestamp {
			opts.Options |= memdx.MetaOpFlagForceAcceptWithMetaOps
		}

		if in.VbUuid != nil {
			opts.VBUUID = *in.VbUuid
		}

		result, err := bucketAgent.SetWithMeta(ctx, &opts)
		if err != nil {
			if errors.Is(err, memdx.ErrConflictOrCasMismatch) {
				if checkCas == 0 {
					// if there is no checked cas, this must be a conflict
					return nil, s.errorHandler.NewDocConflictStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
				}

				// we skip conflict resolution for sets with a checked CAS, so this must be a CAS mismatch
				return nil, s.errorHandler.NewDocCasMismatchStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
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
			} else if errors.Is(err, gocbcorex.ErrVbucketUUIDMismatch) {
				return nil, s.errorHandler.NewVbUuidDivergenceStatus(err, in.BucketName, in.ScopeName, in.CollectionName, in.Key).Err()
			}
			return nil, s.errorHandler.NewGenericStatus(err).Err()
		}

		return &internal_xdcr_v1.PushDocumentResponse{
			Cas:   result.Cas,
			Seqno: result.MutationToken.SeqNo,
		}, nil
	}
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
