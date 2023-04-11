package server_v1

import (
	"encoding/json"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type QueryServer struct {
	query_v1.UnimplementedQueryServiceServer

	logger      *zap.Logger
	authHandler *AuthHandler
}

func NewQueryServer(
	logger *zap.Logger,
	authHandler *AuthHandler,
) *QueryServer {
	return &QueryServer{
		logger:      logger,
		authHandler: authHandler,
	}
}

func (s *QueryServer) Query(in *query_v1.QueryRequest, out query_v1.QueryService_QueryServer) error {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(out.Context(), in.BucketName)
	if errSt != nil {
		return errSt.Err()
	}

	var opts gocbcorex.QueryOptions
	opts.OnBehalfOf = oboInfo

	opts.Statement = in.Statement

	// metrics are included by default
	opts.Metrics = true

	if in.ReadOnly != nil {
		opts.ReadOnly = *in.ReadOnly
	}

	if in.FlexIndex != nil {
		opts.UseFts = *in.FlexIndex
	}

	if in.PreserveExpiry != nil {
		opts.PreserveExpiry = *in.PreserveExpiry
	}

	if in.ConsistentWith != nil {
		return newUnsupportedFieldStatus("ConsistentWith").Err()
	}

	if in.TuningOptions != nil {
		if in.TuningOptions.MaxParallelism != nil {
			opts.MaxParallelism = *in.TuningOptions.MaxParallelism
		}
		if in.TuningOptions.PipelineBatch != nil {
			opts.PipelineBatch = *in.TuningOptions.PipelineBatch
		}
		if in.TuningOptions.PipelineCap != nil {
			opts.PipelineCap = *in.TuningOptions.PipelineCap
		}
		if in.TuningOptions.ScanWait != nil {
			opts.ScanWait = durationToGo(in.TuningOptions.ScanWait)
		}
		if in.TuningOptions.ScanCap != nil {
			opts.ScanCap = *in.TuningOptions.ScanCap
		}
		if in.TuningOptions.DisableMetrics != nil {
			opts.Metrics = !*in.TuningOptions.DisableMetrics
		}
	}

	if in.ClientContextId != nil {
		opts.ClientContextId = *in.ClientContextId
	}

	if in.ScanConsistency != nil {
		switch *in.ScanConsistency {
		case query_v1.QueryRequest_SCAN_CONSISTENCY_NOT_BOUNDED:
			opts.ScanConsistency = cbqueryx.QueryScanConsistencyNotBounded
		case query_v1.QueryRequest_SCAN_CONSISTENCY_REQUEST_PLUS:
			opts.ScanConsistency = cbqueryx.QueryScanConsistencyRequestPlus
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
		params := make(map[string]json.RawMessage, len(named))
		for k, v := range named {
			params[k] = v
		}
		opts.NamedArgs = params
	}
	if len(pos) > 0 {
		params := make([]json.RawMessage, len(pos))
		for i, p := range pos {
			params[i] = p
		}
		opts.Args = params
	}
	if in.ProfileMode != nil {
		switch *in.ProfileMode {
		case query_v1.QueryRequest_PROFILE_MODE_OFF:
			opts.Profile = cbqueryx.QueryProfileModeOff
		case query_v1.QueryRequest_PROFILE_MODE_PHASES:
			opts.Profile = cbqueryx.QueryProfileModePhases
		case query_v1.QueryRequest_PROFILE_MODE_TIMINGS:
			opts.Profile = cbqueryx.QueryProfileModeTimings
		default:
			return status.Errorf(codes.InvalidArgument, "invalid profile mode option specified")
		}
	}

	var result gocbcorex.QueryResultStream
	var err error
	if in.Prepared != nil && *in.Prepared {
		result, err = agent.PreparedQuery(out.Context(), &opts)
	} else {
		result, err = agent.Query(out.Context(), &opts)
	}
	if err != nil {
		return cbGenericErrToPsStatus(err, s.logger).Err()
	}

	var rowCache [][]byte
	var rowCacheNumBytes int = 0
	const MAX_ROW_BYTES = 1024

	for {
		rowBytes, err := result.ReadRow()
		if err != nil {
			return cbGenericErrToPsStatus(err, s.logger).Err()
		}

		if rowBytes == nil {
			break
		}

		rowNumBytes := len(rowBytes)

		if rowCacheNumBytes+rowNumBytes > MAX_ROW_BYTES {
			// adding this row to the cache would exceed its maximum number of
			// bytes, so we need to evict all these rows...
			err := out.Send(&query_v1.QueryResponse{
				Rows:     rowCache,
				MetaData: nil,
			})
			if err != nil {
				return cbGenericErrToPsStatus(err, s.logger).Err()
			}

			rowCache = nil
			rowCacheNumBytes = 0
		}

		rowCache = append(rowCache, rowBytes)
		rowCacheNumBytes += rowNumBytes
	}

	var psMetaData *query_v1.QueryResponse_MetaData

	metaData, err := result.MetaData()
	if err == nil {
		psMetaData = &query_v1.QueryResponse_MetaData{
			RequestId:       metaData.RequestID,
			ClientContextId: metaData.ClientContextID,
		}

		if opts.Metrics {
			psMetrics := &query_v1.QueryResponse_MetaData_Metrics{
				ElapsedTime:   durationFromGo(metaData.Metrics.ElapsedTime),
				ExecutionTime: durationFromGo(metaData.Metrics.ExecutionTime),
				ResultCount:   metaData.Metrics.ResultCount,
				ResultSize:    metaData.Metrics.ResultSize,
				MutationCount: metaData.Metrics.MutationCount,
				SortCount:     metaData.Metrics.SortCount,
				ErrorCount:    metaData.Metrics.ErrorCount,
				WarningCount:  metaData.Metrics.WarningCount,
			}

			psMetaData.Metrics = psMetrics
		}

		warnings := make([]*query_v1.QueryResponse_MetaData_Warning, len(metaData.Warnings))
		for i, warning := range metaData.Warnings {
			warnings[i] = &query_v1.QueryResponse_MetaData_Warning{
				Code:    warning.Code,
				Message: warning.Message,
			}
		}
		psMetaData.Warnings = warnings

		switch metaData.Status {
		case cbqueryx.QueryStatusRunning:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_RUNNING
		case cbqueryx.QueryStatusSuccess:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_SUCCESS
		case cbqueryx.QueryStatusErrors:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_ERRORS
		case cbqueryx.QueryStatusCompleted:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_COMPLETED
		case cbqueryx.QueryStatusStopped:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_STOPPED
		case cbqueryx.QueryStatusTimeout:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_TIMEOUT
		case cbqueryx.QueryStatusClosed:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_CLOSED
		case cbqueryx.QueryStatusFatal:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_FATAL
		case cbqueryx.QueryStatusAborted:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_ABORTED
		default:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_UNKNOWN
		}

		sig, err := json.Marshal(metaData.Signature)
		if err == nil {
			psMetaData.Signature = sig
		}

		if metaData.Profile != nil {
			profile, err := json.Marshal(metaData.Profile)
			if err == nil {
				psMetaData.Profile = profile
			}
		}
	}

	// if we have any rows or meta-data left to stream, we send that first
	// before we process any errors that occurred.
	if rowCache != nil || psMetaData != nil {
		err := out.Send(&query_v1.QueryResponse{
			Rows:     rowCache,
			MetaData: psMetaData,
		})
		if err != nil {
			return cbGenericErrToPsStatus(err, s.logger).Err()
		}
	}

	return nil
}
