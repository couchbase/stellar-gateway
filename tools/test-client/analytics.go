/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package gocbps

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/couchbase/goprotostellar/genproto/analytics_v1"
)

type AnalyticsScanConsistency uint

const (
	AnalyticsScanConsistencyNotBounded AnalyticsScanConsistency = iota + 1
	AnalyticsScanConsistencyRequestPlus
)

type AnalyticsQueryOptions struct {
	ClientContextID      string
	Priority             bool
	PositionalParameters []interface{}
	NamedParameters      map[string]interface{}
	Readonly             bool
	ScanConsistency      AnalyticsScanConsistency
}

type AnalyticsMetrics struct {
	ElapsedTime      time.Duration
	ExecutionTime    time.Duration
	ResultCount      uint64
	ResultSize       uint64
	MutationCount    uint64
	SortCount        uint64
	ErrorCount       uint64
	WarningCount     uint64
	ProcessedObjects uint64
}

type AnalyticsWarning struct {
	Code    uint32
	Message string
}

type AnalyticsMetaData struct {
	RequestID       string
	ClientContextID string
	Metrics         AnalyticsMetrics
	Signature       interface{}
	Warnings        []AnalyticsWarning
}

type AnalyticsQueryResult struct {
	client analytics_v1.AnalyticsService_AnalyticsQueryClient

	rowCounter int
	nextRows   [][]byte
	meta       *AnalyticsMetaData
	err        error
}

func (r *AnalyticsQueryResult) populateMeta(metadata *analytics_v1.AnalyticsQueryResponse_MetaData) {
	meta := &AnalyticsMetaData{
		RequestID:       metadata.RequestId,
		ClientContextID: metadata.ClientContextId,
		Warnings:        nil,
	}

	if len(metadata.Signature) > 0 {
		err := json.Unmarshal(metadata.Signature, &meta.Signature)
		if err != nil {
			r.err = err
		}
	}

	if metadata.Metrics != nil {
		meta.Metrics = AnalyticsMetrics{
			ElapsedTime:      metadata.Metrics.ElapsedTime.AsDuration(),
			ExecutionTime:    metadata.Metrics.ExecutionTime.AsDuration(),
			ResultCount:      metadata.Metrics.ResultCount,
			ResultSize:       metadata.Metrics.ResultSize,
			MutationCount:    metadata.Metrics.MutationCount,
			SortCount:        metadata.Metrics.SortCount,
			ErrorCount:       metadata.Metrics.ErrorCount,
			WarningCount:     metadata.Metrics.WarningCount,
			ProcessedObjects: metadata.Metrics.ProcessedObjects,
		}
	}

	meta.Warnings = make([]AnalyticsWarning, len(metadata.Warnings))
	for i, warning := range metadata.Warnings {
		meta.Warnings[i] = AnalyticsWarning{
			Code:    warning.Code,
			Message: warning.Message,
		}
	}

	r.meta = meta
}

func (r *AnalyticsQueryResult) Next() bool {
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

func (r *AnalyticsQueryResult) Row() ([]byte, error) {
	if len(r.nextRows) == 0 {
		return nil, errors.New("no rows")
	}

	val := r.nextRows[r.rowCounter]

	return val, nil
}

func (r *AnalyticsQueryResult) One() ([]byte, error) {
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

func (r *AnalyticsQueryResult) Err() error {
	return r.err
}

func (r *AnalyticsQueryResult) MetaData() (*AnalyticsMetaData, error) {
	if r.meta == nil {
		return nil, errors.New("no metadata")
	}

	return r.meta, nil
}

func (r *AnalyticsQueryResult) Close() error {
	return r.client.CloseSend()
}

func (c *Client) AnalyticsQuery(ctx context.Context, statement string, opts *AnalyticsQueryOptions) (*AnalyticsQueryResult, error) {
	if opts == nil {
		opts = &AnalyticsQueryOptions{}
	}

	req := &analytics_v1.AnalyticsQueryRequest{
		Statement: statement,
	}
	if opts.ClientContextID != "" {
		req.ClientContextId = &opts.ClientContextID
	}
	if opts.Readonly {
		req.ReadOnly = &opts.Readonly
	}
	if opts.Priority {
		req.Priority = &opts.Priority
	}
	if opts.ScanConsistency > 0 {
		var consistency analytics_v1.AnalyticsQueryRequest_ScanConsistency
		switch opts.ScanConsistency {
		case AnalyticsScanConsistencyNotBounded:
			consistency = analytics_v1.AnalyticsQueryRequest_SCAN_CONSISTENCY_NOT_BOUNDED
		case AnalyticsScanConsistencyRequestPlus:
			consistency = analytics_v1.AnalyticsQueryRequest_SCAN_CONSISTENCY_REQUEST_PLUS
		}
		req.ScanConsistency = &consistency
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
	res, err := c.analyticsClient.AnalyticsQuery(ctx, req)
	if err != nil {
		return nil, err
	}

	return &AnalyticsQueryResult{
		client: res,
	}, nil
}
