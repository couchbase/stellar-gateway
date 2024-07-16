package server_v1

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"github.com/couchbase/stellar-gateway/gateway/apiversion"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type QueryServer struct {
	query_v1.UnimplementedQueryServiceServer

	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
}

func NewQueryServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
) *QueryServer {
	return &QueryServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
	}
}

func (s *QueryServer) translateError(err error) *status.Status {
	var queryErrs *cbqueryx.ServerErrors
	if errors.As(err, &queryErrs) {
		if len(queryErrs.Errors) == 0 {
			return s.errorHandler.NewInternalStatus()
		}

		firstErr := queryErrs.Errors[0]
		if errors.Is(firstErr, cbqueryx.ErrParsingFailure) {
			return s.errorHandler.NewInvalidQueryStatus(err, firstErr.Msg)
		} else if errors.Is(firstErr, cbqueryx.ErrAuthenticationFailure) {
			return s.errorHandler.NewQueryNoAccessStatus(err)
		} else if errors.Is(err, cbqueryx.ErrWriteInReadOnlyQuery) {
			return s.errorHandler.NewWriteInReadOnlyQueryStatus(err)
		}

		var rErr *cbqueryx.ResourceError
		if errors.As(firstErr, &rErr) {
			if errors.Is(err, cbqueryx.ErrIndexExists) {
				return s.errorHandler.NewQueryIndexExistsStatus(err, rErr.IndexName, "", "", "")
			} else if errors.Is(err, cbqueryx.ErrIndexNotFound) {
				return s.errorHandler.NewQueryIndexMissingStatus(err, rErr.IndexName, "", "", "")
			}
		}
	}

	return s.errorHandler.NewGenericStatus(err)
}

func (s *QueryServer) Query(in *query_v1.QueryRequest, out query_v1.QueryService_QueryServer) error {
	if in.DurabilityLevel != nil {
		errSt := checkApiVersion(out.Context(), apiversion.QueryDurabilityLevel, "DurabilityLevel")
		if errSt != nil {
			return errSt.Err()
		}
	}

	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(out.Context(), in.BucketName)
	if errSt != nil {
		return errSt.Err()
	}

	var opts gocbcorex.QueryOptions
	opts.OnBehalfOf = oboInfo

	opts.Statement = in.Statement

	// metrics are included by default
	opts.Metrics = true

	if in.BucketName == nil && in.ScopeName != nil {
		return status.Errorf(codes.InvalidArgument, "invalid scope and bucket name combination options specified")
	}

	if in.BucketName != nil && in.ScopeName != nil {
		opts.QueryContext = fmt.Sprintf("`%s`.`%s`", in.GetBucketName(), in.GetScopeName())
	}

	if in.DurabilityLevel != nil {
		durabilityLevel, errSt := durabilityLevelToCbqueryx(*in.DurabilityLevel)
		if errSt != nil {
			return errSt.Err()
		}
		opts.DurabilityLevel = durabilityLevel
	}

	if in.ReadOnly != nil {
		opts.ReadOnly = *in.ReadOnly
	}

	if in.FlexIndex != nil {
		opts.UseFts = *in.FlexIndex
	}

	if in.PreserveExpiry != nil {
		opts.PreserveExpiry = *in.PreserveExpiry
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
	} else {
		opts.ClientContextId = uuid.NewString()
	}

	if in.ScanConsistency != nil && len(in.ConsistentWith) > 0 {
		return status.Errorf(codes.InvalidArgument, "cannot specify both token-based and enumeration-based consistency")
	} else if in.ScanConsistency != nil {
		scanConsistency, errSt := scanConsistencyToCbqueryx(*in.ScanConsistency)
		if errSt != nil {
			return errSt.Err()
		}
		opts.ScanConsistency = scanConsistency
	} else if len(in.ConsistentWith) > 0 {
		vectors := make(map[string]cbqueryx.SparseScanVectors)
		for _, vector := range in.ConsistentWith {
			bucketVectors := vectors[vector.BucketName]
			if bucketVectors == nil {
				bucketVectors = make(cbqueryx.SparseScanVectors)
				vectors[vector.BucketName] = bucketVectors
			}

			bucketVectors[vector.VbucketId] = cbqueryx.ScanVectorEntry{
				SeqNo:  vector.SeqNo,
				VbUuid: fmt.Sprintf("%d", vector.VbucketUuid),
			}
		}

		jsonVectors := make(map[string]json.RawMessage)
		for bucketName, bucketVectors := range vectors {
			bucketJson, err := json.Marshal(bucketVectors)
			if err != nil {
				return s.errorHandler.NewGenericStatus(err).Err()
			}

			jsonVectors[bucketName] = bucketJson
		}

		opts.ScanConsistency = cbqueryx.ScanConsistencyAtPlus
		opts.ScanVectors = jsonVectors
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
			opts.Profile = cbqueryx.ProfileModeOff
		case query_v1.QueryRequest_PROFILE_MODE_PHASES:
			opts.Profile = cbqueryx.ProfileModePhases
		case query_v1.QueryRequest_PROFILE_MODE_TIMINGS:
			opts.Profile = cbqueryx.ProfileModeTimings
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
		return s.translateError(err).Err()
	}

	var rowCache [][]byte
	rowCacheNumBytes := 0
	const MaxRowBytes = 1024

	for result.HasMoreRows() {
		rowBytes, err := result.ReadRow()
		if err != nil {
			return s.translateError(err).Err()
		}

		rowNumBytes := len(rowBytes)

		if rowCacheNumBytes+rowNumBytes > MaxRowBytes {
			// adding this row to the cache would exceed its maximum number of
			// bytes, so we need to evict all these rows...
			err := out.Send(&query_v1.QueryResponse{
				Rows:     rowCache,
				MetaData: nil,
			})
			if err != nil {
				return s.errorHandler.NewGenericStatus(err).Err()
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
		case cbqueryx.StatusRunning:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_RUNNING
		case cbqueryx.StatusSuccess:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_SUCCESS
		case cbqueryx.StatusErrors:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_ERRORS
		case cbqueryx.StatusCompleted:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_COMPLETED
		case cbqueryx.StatusStopped:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_STOPPED
		case cbqueryx.StatusTimeout:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_TIMEOUT
		case cbqueryx.StatusClosed:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_CLOSED
		case cbqueryx.StatusFatal:
			psMetaData.Status = query_v1.QueryResponse_MetaData_STATUS_FATAL
		case cbqueryx.StatusAborted:
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
			return s.errorHandler.NewGenericStatus(err).Err()
		}
	}

	return nil
}
