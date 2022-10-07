package server_v1

import (
	"encoding/json"

	"github.com/couchbase/gocb/v2"
	query_v1 "github.com/couchbase/stellar-nebula/genproto/query/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type QueryServer struct {
	query_v1.UnimplementedQueryServer

	cbClient *gocb.Cluster
}

func (s *QueryServer) Query(in *query_v1.QueryRequest, out query_v1.Query_QueryServer) error {
	var opts gocb.QueryOptions

	// metrics are included by default
	opts.Metrics = true

	if in.ReadOnly != nil {
		opts.Readonly = *in.ReadOnly
	}

	if in.Prepared != nil {
		opts.Adhoc = !*in.Prepared
	}

	if in.FlexIndex != nil {
		opts.FlexIndex = *in.FlexIndex
	}

	if in.PreserveExpiry != nil {
		opts.PreserveExpiry = *in.PreserveExpiry
	}

	if in.ConsistentWith != nil {
		opts.ConsistentWith = gocb.NewMutationState()
		// TODO(chvck): gocb doesn't expose a way to create mutation tokens beyond json marshal/unmarshal
		// for _, token := range in.ConsistentWith.Tokens {
		// 	opts.ConsistentWith.Add(gocb.MutationToken{})
		// }
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
			opts.ScanWait = durationFromPs(in.TuningOptions.ScanWait)
		}
		if in.TuningOptions.ScanCap != nil {
			opts.ScanCap = *in.TuningOptions.ScanCap
		}
		if in.TuningOptions.DisableMetrics != nil {
			opts.Metrics = !*in.TuningOptions.DisableMetrics
		}
	}

	if in.ClientContextId != nil {
		opts.ClientContextID = *in.ClientContextId
	}

	if in.ScanConsistency != nil {
		switch *in.ScanConsistency {
		case query_v1.QueryRequest_NOT_BOUNDED:
			opts.ScanConsistency = gocb.QueryScanConsistencyNotBounded
		case query_v1.QueryRequest_REQUEST_PLUS:
			opts.ScanConsistency = gocb.QueryScanConsistencyRequestPlus
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
	if in.ProfileMode != nil {
		switch *in.ProfileMode {
		case query_v1.QueryRequest_OFF:
			opts.Profile = gocb.QueryProfileModeNone
		case query_v1.QueryRequest_PHASES:
			opts.Profile = gocb.QueryProfileModePhases
		case query_v1.QueryRequest_TIMINGS:
			opts.Profile = gocb.QueryProfileModeTimings
		default:
			return status.Errorf(codes.InvalidArgument, "invalid profile mode option specified")
		}
	}

	result, err := s.cbClient.Query(in.Statement, &opts)
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
			err := out.Send(&query_v1.QueryResponse{
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

	var psMetaData *query_v1.QueryResponse_MetaData

	metaData, err := result.MetaData()
	if err == nil {
		psMetaData = &query_v1.QueryResponse_MetaData{
			RequestId:       metaData.RequestID,
			ClientContextId: metaData.ClientContextID,
		}

		if opts.Metrics {
			psMetrics := &query_v1.QueryResponse_MetaData_Metrics{
				ElapsedTime:   durationToPs(metaData.Metrics.ElapsedTime),
				ExecutionTime: durationToPs(metaData.Metrics.ExecutionTime),
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
		case gocb.QueryStatusRunning:
			psMetaData.Status = query_v1.QueryResponse_MetaData_RUNNING
		case gocb.QueryStatusSuccess:
			psMetaData.Status = query_v1.QueryResponse_MetaData_SUCCESS
		case gocb.QueryStatusErrors:
			psMetaData.Status = query_v1.QueryResponse_MetaData_ERRORS
		case gocb.QueryStatusCompleted:
			psMetaData.Status = query_v1.QueryResponse_MetaData_COMPLETED
		case gocb.QueryStatusStopped:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STOPPED
		case gocb.QueryStatusTimeout:
			psMetaData.Status = query_v1.QueryResponse_MetaData_TIMEOUT
		case gocb.QueryStatusClosed:
			psMetaData.Status = query_v1.QueryResponse_MetaData_CLOSED
		case gocb.QueryStatusFatal:
			psMetaData.Status = query_v1.QueryResponse_MetaData_FATAL
		case gocb.QueryStatusAborted:
			psMetaData.Status = query_v1.QueryResponse_MetaData_ABORTED
		default:
			psMetaData.Status = query_v1.QueryResponse_MetaData_UNKNOWN
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
			return cbErrToPs(err)
		}
	}

	err = result.Err()
	if err != nil {
		return cbErrToPs(err)
	}

	return nil
}

func NewQueryServer(cbClient *gocb.Cluster) *QueryServer {
	return &QueryServer{
		cbClient: cbClient,
	}
}
