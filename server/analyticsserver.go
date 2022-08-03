package server

import (
	"encoding/json"

	"github.com/couchbase/gocb/v2"
	analytics_v1 "github.com/couchbase/stellar-nebula/genproto/analytics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type analyticsServer struct {
	analytics_v1.UnimplementedAnalyticsServer

	cbClient *gocb.Cluster
}

func (s *analyticsServer) AnalyticsQuery(in *analytics_v1.AnalyticsQueryRequest, out analytics_v1.Analytics_AnalyticsQueryServer) error {
	var opts gocb.AnalyticsOptions

	if in.ReadOnly != nil {
		opts.Readonly = *in.ReadOnly
	}
	if in.ClientContextId != nil {
		opts.ClientContextID = *in.ClientContextId
	}
	if in.Priority != nil {
		opts.Priority = *in.Priority
	}
	if in.ScanConsistency != nil {
		switch *in.ScanConsistency {
		case analytics_v1.AnalyticsQueryRequest_NOT_BOUNDED:
			opts.ScanConsistency = gocb.AnalyticsScanConsistencyNotBounded
		case analytics_v1.AnalyticsQueryRequest_REQUEST_PLUS:
			opts.ScanConsistency = gocb.AnalyticsScanConsistencyRequestPlus
		default:
			return status.Errorf(codes.InvalidArgument, "invalid scan consistency option specified")
		}
	}
	named := in.GetNamedParameters()
	pos := in.GetPositionalParameters()
	if len(named) > 0 && len(pos) > 0 {
		return status.Errorf(codes.InvalidArgument, "named and positional parameters must be used exclusively")
	}
	if len(named) > 0 {
		params := make(map[string]interface{}, len(named))
		for k, param := range named {
			var p interface{}
			if err := json.Unmarshal(param, &p); err != nil {
				return cbErrToPs(err)
			}

			params[k] = p
		}
		opts.NamedParameters = params
	}
	if len(pos) > 0 {
		params := make([]interface{}, len(pos))
		for i, param := range pos {
			var p interface{}
			if err := json.Unmarshal(param, &p); err != nil {
				return cbErrToPs(err)
			}

			params[i] = p
		}
		opts.PositionalParameters = params
	}

	result, err := s.cbClient.AnalyticsQuery(in.Statement, &opts)
	if err != nil {
		return cbErrToPs(err)
	}

	var rowCache [][]byte
	var rowCacheNumBytes int = 0
	const MAX_ROW_BYTES = 1024

	for result.Next() {
		var rowBytes json.RawMessage
		err := result.Row(&rowBytes)
		if err != nil {
			return cbErrToPs(err)
		}

		rowNumBytes := len(rowBytes)

		if rowCacheNumBytes+rowNumBytes > MAX_ROW_BYTES {
			// adding this row to the cache would exceed its maximum number of
			// bytes, so we need to evict all these rows...
			err := out.Send(&analytics_v1.AnalyticsQueryResponse{
				Rows:     rowCache,
				MetaData: nil,
			})
			if err != nil {
				return cbErrToPs(err)
			}

			rowCache = nil
			rowCacheNumBytes = 0
		}

		rowCache = append(rowCache, rowBytes)
		rowCacheNumBytes += rowNumBytes
	}

	var psMetaData *analytics_v1.AnalyticsQueryResponse_MetaData

	metaData, err := result.MetaData()
	if err == nil {
		psMetrics := &analytics_v1.AnalyticsQueryResponse_Metrics{
			ElapsedTime:      durationToPs(metaData.Metrics.ElapsedTime),
			ExecutionTime:    durationToPs(metaData.Metrics.ExecutionTime),
			ResultCount:      metaData.Metrics.ResultCount,
			ResultSize:       metaData.Metrics.ResultSize,
			MutationCount:    metaData.Metrics.MutationCount,
			SortCount:        metaData.Metrics.SortCount,
			ErrorCount:       metaData.Metrics.ErrorCount,
			WarningCount:     metaData.Metrics.WarningCount,
			ProcessedObjects: metaData.Metrics.ProcessedObjects,
		}

		warnings := make([]*analytics_v1.AnalyticsQueryResponse_MetaData_Warning, len(metaData.Warnings))
		for i, warning := range metaData.Warnings {
			warnings[i] = &analytics_v1.AnalyticsQueryResponse_MetaData_Warning{
				Code:    warning.Code,
				Message: warning.Message,
			}
		}

		psMetaData = &analytics_v1.AnalyticsQueryResponse_MetaData{
			RequestId:       metaData.RequestID,
			ClientContextId: metaData.ClientContextID,
			Metrics:         psMetrics,
			Warnings:        warnings,
			// Status:          metaData.Status,	TODO(chvck): we probably should include status but gocb hides that detail
		}

		sig, err := json.Marshal(metaData.Signature)
		if err == nil {
			psMetaData.Signature = sig
		}
	}

	// if we have any rows or meta-data left to stream, we send that first
	// before we process any errors that occurred.
	if rowCache != nil || psMetaData != nil {
		err := out.Send(&analytics_v1.AnalyticsQueryResponse{
			Rows:     rowCache,
			MetaData: psMetaData,
		})
		if err != nil {
			return cbErrToPs(err)
		}
	}

	err = result.Err()
	if err != nil {
		return cbErrToPs(err)
	}

	return nil
}

func NewAnalyticsServer(cbClient *gocb.Cluster) *analyticsServer {
	return &analyticsServer{
		cbClient: cbClient,
	}
}
