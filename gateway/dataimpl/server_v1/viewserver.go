package server_v1

import (
	"encoding/json"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/goprotostellar/genproto/view_v1"
)

type ViewServer struct {
	view_v1.UnimplementedViewServiceServer
	logger   *zap.Logger
	cbClient *gocb.Cluster
}

func (s *ViewServer) ViewQuery(in *view_v1.ViewQueryRequest, out view_v1.ViewService_ViewQueryServer) error {
	var opts gocb.ViewOptions

	if in.ScanConsistency != nil {
		switch *in.ScanConsistency {
		case view_v1.ViewQueryRequest_SCAN_CONSISTENCY_NOT_BOUNDED:
			opts.ScanConsistency = gocb.ViewScanConsistencyNotBounded
		case view_v1.ViewQueryRequest_SCAN_CONSISTENCY_REQUEST_PLUS:
			opts.ScanConsistency = gocb.ViewScanConsistencyRequestPlus
		case view_v1.ViewQueryRequest_SCAN_CONSISTENCY_UPDATE_AFTER:
			opts.ScanConsistency = gocb.ViewScanConsistencyUpdateAfter
		default:
			return status.Errorf(codes.InvalidArgument, "invalid scan consistency option specified")
		}
	}
	if in.Skip != nil {
		opts.Skip = *in.Skip
	}
	if in.Limit != nil {
		opts.Limit = *in.Limit
	}
	if in.Order != nil {
		switch *in.Order {
		case view_v1.ViewQueryRequest_ORDER_ASCENDING:
			opts.Order = gocb.ViewOrderingAscending
		case view_v1.ViewQueryRequest_ORDER_DESCENDING:
			opts.Order = gocb.ViewOrderingDescending
		default:
			return status.Errorf(codes.InvalidArgument, "invalid order option specified")
		}
	}
	if in.Reduce != nil {
		opts.Reduce = *in.Reduce
	}
	if in.Group != nil {
		opts.Group = *in.Group
	}
	if in.GroupLevel != nil {
		opts.GroupLevel = *in.GroupLevel
	}
	if len(in.Key) > 0 {
		var key interface{}
		if err := json.Unmarshal(in.Key, &key); err != nil {
			return cbGenericErrToPsStatus(err, s.logger).Err()
		}
		opts.Key = key
	}
	if len(in.Keys) > 0 {
		keys := make([]interface{}, len(in.Keys))
		for i, k := range in.Keys {
			var key interface{}
			if err := json.Unmarshal(k, &key); err != nil {
				return cbGenericErrToPsStatus(err, s.logger).Err()
			}

			keys[i] = key
		}
		opts.Keys = keys
	}
	if len(in.StartKey) > 0 {
		var key interface{}
		if err := json.Unmarshal(in.StartKey, &key); err != nil {
			return cbGenericErrToPsStatus(err, s.logger).Err()
		}
		opts.StartKey = key
	}
	if len(in.EndKey) > 0 {
		var key interface{}
		if err := json.Unmarshal(in.EndKey, &key); err != nil {
			return cbGenericErrToPsStatus(err, s.logger).Err()
		}
		opts.EndKey = key
	}
	if in.InclusiveEnd != nil {
		opts.InclusiveEnd = *in.InclusiveEnd
	}
	if in.StartKeyDocId != nil {
		opts.StartKeyDocID = *in.StartKeyDocId
	}
	if in.EndKeyDocId != nil {
		opts.EndKeyDocID = *in.EndKeyDocId
	}
	if in.OnError != nil {
		switch *in.OnError {
		case view_v1.ViewQueryRequest_ERROR_MODE_CONTINUE:
			opts.OnError = gocb.ViewErrorModeContinue
		case view_v1.ViewQueryRequest_ERROR_MODE_STOP:
			opts.OnError = gocb.ViewErrorModeStop
		default:
			return status.Errorf(codes.InvalidArgument, "invalid on error option specified")
		}
	}
	if in.Debug != nil {
		opts.Debug = *in.Debug
	}

	result, err := s.cbClient.Bucket(in.BucketName).ViewQuery(in.DesignDocumentName, in.ViewName, &opts)
	if err != nil {
		return cbGenericErrToPsStatus(err, s.logger).Err()
	}

	var rowCache []*view_v1.ViewQueryResponse_Row
	var rowCacheNumBytes int = 0
	const MAX_ROW_BYTES = 1024

	for result.Next() {
		row := result.Row()

		var rowKeyBytes json.RawMessage
		err := row.Key(&rowKeyBytes)
		if err != nil {
			return cbGenericErrToPsStatus(err, s.logger).Err()
		}
		var rowValBytes json.RawMessage
		err = row.Value(&rowValBytes)
		if err != nil {
			return cbGenericErrToPsStatus(err, s.logger).Err()
		}

		rowNumBytes := len(rowKeyBytes) + len(rowValBytes) + len(row.ID)

		if rowCacheNumBytes+rowNumBytes > MAX_ROW_BYTES {
			// adding this row to the cache would exceed its maximum number of
			// bytes, so we need to evict all these rows...
			err := out.Send(&view_v1.ViewQueryResponse{
				Rows:     rowCache,
				MetaData: nil,
			})
			if err != nil {
				return cbGenericErrToPsStatus(err, s.logger).Err()
			}

			rowCache = nil
			rowCacheNumBytes = 0
		}

		rowCache = append(rowCache, &view_v1.ViewQueryResponse_Row{
			Id:    row.ID,
			Key:   rowKeyBytes,
			Value: rowValBytes,
		})
		rowCacheNumBytes += rowNumBytes
	}

	var psMetaData *view_v1.ViewQueryResponse_MetaData

	metaData, err := result.MetaData()
	if err == nil {
		psMetaData = &view_v1.ViewQueryResponse_MetaData{
			TotalRows: metaData.TotalRows,
		}

		dbg, err := json.Marshal(metaData.Debug)
		if err == nil {
			psMetaData.Debug = dbg
		}
	}

	// if we have any rows or meta-data left to stream, we send that first
	// before we process any errors that occurred.
	if rowCache != nil || psMetaData != nil {
		err := out.Send(&view_v1.ViewQueryResponse{
			Rows:     rowCache,
			MetaData: psMetaData,
		})
		if err != nil {
			return cbGenericErrToPsStatus(err, s.logger).Err()
		}
	}

	err = result.Err()
	if err != nil {
		return cbGenericErrToPsStatus(err, s.logger).Err()
	}

	return nil
}

func NewViewServer(cbClient *gocb.Cluster, logger *zap.Logger) *ViewServer {
	return &ViewServer{
		cbClient: cbClient,
		logger:   logger,
	}
}
