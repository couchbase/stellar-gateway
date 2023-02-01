package gocbps

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/query_v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

type QueryScanConsistency uint

const (
	QueryScanConsistencyNotBounded QueryScanConsistency = iota + 1
	QueryScanConsistencyRequestPlus
)

type QueryProfileMode string

const (
	QueryProfileModeNone    QueryProfileMode = "off"
	QueryProfileModePhases  QueryProfileMode = "phases"
	QueryProfileModeTimings QueryProfileMode = "timings"
)

type QueryOptions struct {
	ClientContextID      string
	PositionalParameters []interface{}
	NamedParameters      map[string]interface{}
	Readonly             bool
	ScanConsistency      QueryScanConsistency
	ConsistentWith       *MutationState
	Profile              QueryProfileMode
	ScanCap              uint32
	PipelineBatch        uint32
	PipelineCap          uint32
	ScanWait             time.Duration
	MaxParallelism       uint32
	Metrics              bool
	AdHoc                bool
	FlexIndex            bool
	PreserveExpiry       bool
}

type QueryMetrics struct {
	ElapsedTime   time.Duration
	ExecutionTime time.Duration
	ResultCount   uint64
	ResultSize    uint64
	MutationCount uint64
	SortCount     uint64
	ErrorCount    uint64
	WarningCount  uint64
}

type QueryWarning struct {
	Code    uint32
	Message string
}

type QueryMetaData struct {
	RequestID       string
	ClientContextID string
	Metrics         QueryMetrics
	Signature       interface{}
	Profile         interface{}
	Warnings        []QueryWarning
	Status          string
}

type QueryResult struct {
	client query_v1.Query_QueryClient

	rowCounter int
	nextRows   [][]byte
	meta       *QueryMetaData
	err        error
}

func (r *QueryResult) populateMeta(metadata *query_v1.QueryResponse_MetaData) {
	meta := &QueryMetaData{
		RequestID:       metadata.RequestId,
		ClientContextID: metadata.ClientContextId,
		Status:          strings.ToLower(metadata.Status.String()),
	}

	if len(metadata.Signature) > 0 {
		err := json.Unmarshal(metadata.Signature, &meta.Signature)
		if err != nil {
			r.err = err
		}
	}
	if len(metadata.Profile) > 0 {
		err := json.Unmarshal(metadata.Profile, &meta.Profile)
		if err != nil {
			r.err = err
		}
	}

	if metadata.Metrics != nil {
		meta.Metrics = QueryMetrics{
			ElapsedTime:   metadata.Metrics.ElapsedTime.AsDuration(),
			ExecutionTime: metadata.Metrics.ExecutionTime.AsDuration(),
			ResultCount:   metadata.Metrics.ResultCount,
			ResultSize:    metadata.Metrics.ResultSize,
			MutationCount: metadata.Metrics.MutationCount,
			SortCount:     metadata.Metrics.SortCount,
			ErrorCount:    metadata.Metrics.ErrorCount,
			WarningCount:  metadata.Metrics.WarningCount,
		}
	}

	meta.Warnings = make([]QueryWarning, len(metadata.Warnings))
	for i, warning := range metadata.Warnings {
		meta.Warnings[i] = QueryWarning{
			Code:    warning.Code,
			Message: warning.Message,
		}
	}

	r.meta = meta
}

func (r *QueryResult) Next() bool {
	r.rowCounter++
	if r.rowCounter < len(r.nextRows) {
		return true
	}

	next, err := r.client.Recv()
	if err != nil {
		if !errors.Is(err, io.EOF) {
			r.err = err
		}
		return false
	}

	r.rowCounter = 0

	r.nextRows = next.Rows
	if next.MetaData != nil {
		r.populateMeta(next.MetaData)
	}

	return len(r.nextRows) > 0
}

func (r *QueryResult) Row() ([]byte, error) {
	if len(r.nextRows) == 0 {
		return nil, errors.New("no rows")
	}

	val := r.nextRows[r.rowCounter]

	return val, nil
}

func (r *QueryResult) One() ([]byte, error) {
	next, err := r.client.Recv()
	if err != nil {
		return nil, err
	}
	if len(next.Rows) == 0 {
		return nil, errors.New("no rows")
	}

	val := next.Rows[0]
	for r.Next() {
	}

	return val, nil
}

func (r *QueryResult) Err() error {
	return r.err
}

func (r *QueryResult) MetaData() (*QueryMetaData, error) {
	if r.meta == nil {
		return nil, errors.New("no metadata")
	}

	return r.meta, nil
}

func (r *QueryResult) Close() error {
	return r.client.CloseSend()
}

func (c *Client) Query(ctx context.Context, statement string, opts *QueryOptions) (*QueryResult, error) {
	if opts == nil {
		opts = &QueryOptions{}
	}

	req := &query_v1.QueryRequest{
		Statement: statement,
	}
	if opts.ClientContextID != "" {
		req.ClientContextId = &opts.ClientContextID
	}
	if opts.Readonly {
		req.ReadOnly = &opts.Readonly
	}
	if opts.PreserveExpiry {
		req.PreserveExpiry = &opts.PreserveExpiry
	}
	if opts.FlexIndex {
		req.FlexIndex = &opts.FlexIndex
	}
	prepared := !opts.AdHoc
	req.Prepared = &prepared

	if opts.ScanConsistency > 0 {
		var consistency query_v1.QueryRequest_QueryScanConsistency
		switch opts.ScanConsistency {
		case QueryScanConsistencyNotBounded:
			consistency = query_v1.QueryRequest_NOT_BOUNDED
		case QueryScanConsistencyRequestPlus:
			consistency = query_v1.QueryRequest_REQUEST_PLUS
		}
		req.ScanConsistency = &consistency
	}
	if opts.Profile != "" {
		var profile query_v1.QueryRequest_QueryProfileMode
		switch opts.Profile {
		case QueryProfileModeNone:
			profile = query_v1.QueryRequest_OFF
		case QueryProfileModePhases:
			profile = query_v1.QueryRequest_PHASES
		case QueryProfileModeTimings:
			profile = query_v1.QueryRequest_TIMINGS
		}
		req.ProfileMode = &profile
	}
	if len(opts.NamedParameters) > 0 {
		params := make(map[string][]byte, len(opts.NamedParameters))
		for k, param := range opts.NamedParameters {
			b, err := json.Marshal(param)
			if err != nil {
				return nil, err
			}

			params[k] = b
		}

		req.NamedParameters = params
	}
	if len(opts.PositionalParameters) > 0 {
		params := make([][]byte, len(opts.PositionalParameters))
		for i, param := range opts.PositionalParameters {
			b, err := json.Marshal(param)
			if err != nil {
				return nil, err
			}

			params[i] = b
		}

		req.PositionalParameters = params
	}
	req.TuningOptions = &query_v1.QueryRequest_TuningOptions{}
	if opts.MaxParallelism > 0 {
		req.TuningOptions.MaxParallelism = &opts.MaxParallelism
	}
	if opts.PipelineBatch > 0 {
		req.TuningOptions.PipelineBatch = &opts.PipelineBatch
	}
	if opts.PipelineCap > 0 {
		req.TuningOptions.PipelineCap = &opts.PipelineCap
	}
	if opts.ScanWait > 0 {
		req.TuningOptions.ScanWait = durationpb.New(opts.ScanWait)
	}
	if opts.ScanCap > 0 {
		req.TuningOptions.ScanCap = &opts.ScanCap
	}
	disableMetrics := !opts.Metrics
	req.TuningOptions.DisableMetrics = &disableMetrics

	if opts.ConsistentWith != nil {
		tokens := make([]*kv_v1.MutationToken, len(opts.ConsistentWith.Tokens))
		for i, token := range opts.ConsistentWith.Tokens {
			tokens[i] = &kv_v1.MutationToken{
				BucketName:  token.BucketName,
				VbucketId:   uint32(token.VbID),
				VbucketUuid: token.VbUUID,
				SeqNo:       token.SeqNo,
			}
		}
		req.ConsistentWith = tokens
	}

	res, err := c.queryClient.Query(ctx, req)
	if err != nil {
		return nil, err
	}

	return &QueryResult{
		client: res,
	}, nil
}
