package gocbps

import (
	"context"
	"encoding/json"
	"errors"
	view_v1 "github.com/couchbase/stellar-nebula/genproto/view/v1"
	"io"
)

type ViewScanConsistency uint

const (
	ViewScanConsistencyNotBounded ViewScanConsistency = iota + 1
	ViewScanConsistencyRequestPlus
	ViewScanConsistencyUpdateAfter
)

type ViewOrdering uint

const (
	ViewOrderingAscending ViewOrdering = iota + 1
	ViewOrderingDescending
)

type ViewErrorMode uint

const (
	ViewErrorModeContinue ViewErrorMode = iota + 1
	ViewErrorModeStop
)

type DesignDocumentNamespace uint

const (
	DesignDocumentNamespaceProduction DesignDocumentNamespace = iota + 1
	DesignDocumentNamespaceDevelopment
)

type ViewOptions struct {
	ScanConsistency ViewScanConsistency
	Skip            uint32
	Limit           uint32
	Order           ViewOrdering
	Reduce          bool
	Group           bool
	GroupLevel      uint32
	Key             interface{}
	Keys            []interface{}
	StartKey        interface{}
	EndKey          interface{}
	InclusiveEnd    bool
	StartKeyDocID   string
	EndKeyDocID     string
	OnError         ViewErrorMode
	Debug           bool
	Namespace       DesignDocumentNamespace
}

type ViewMetaData struct {
	TotalRows uint64
	Debug     interface{}
}

type ViewRow struct {
	ID    string
	Key   []byte
	Value []byte
}

type ViewResult struct {
	client view_v1.View_ViewQueryClient

	rowCounter int
	nextRows   []ViewRow
	meta       *ViewMetaData
	err        error
}

func (r *ViewResult) Next() bool {
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

	r.nextRows = make([]ViewRow, len(next.Rows))
	for i, row := range next.Rows {
		r.nextRows[i] = ViewRow{
			ID:    row.Id,
			Key:   row.Key,
			Value: row.Value,
		}
	}

	if next.MetaData != nil {
		r.meta = &ViewMetaData{
			TotalRows: next.MetaData.TotalRows,
		}

		if len(next.MetaData.Debug) > 0 {
			err := json.Unmarshal(next.MetaData.Debug, &r.meta.Debug)
			if err != nil {
				r.err = err
			}
		}
	}

	return len(r.nextRows) > 0
}

func (r *ViewResult) Row() ViewRow {
	if len(r.nextRows) == 0 {
		return ViewRow{}
	}

	val := r.nextRows[r.rowCounter]

	return val
}

func (r *ViewResult) Err() error {
	return r.err
}

func (r *ViewResult) MetaData() (*ViewMetaData, error) {
	if r.meta == nil {
		return nil, errors.New("no metadata")
	}

	return r.meta, nil
}

func (r *ViewResult) Close() error {
	return r.client.CloseSend()
}

func (b *Bucket) ViewQuery(ctx context.Context, designDoc string, viewName string, opts *ViewOptions) (*ViewResult, error) {
	if opts == nil {
		opts = &ViewOptions{}
	}

	req := &view_v1.ViewQueryRequest{
		BucketName:         b.bucketName,
		DesignDocumentName: designDoc,
		ViewName:           viewName,
	}
	if opts.ScanConsistency > 0 {
		var consistency view_v1.ViewQueryRequest_ScanConsistency
		switch opts.ScanConsistency {
		case ViewScanConsistencyNotBounded:
			consistency = view_v1.ViewQueryRequest_NOT_BOUNDED
		case ViewScanConsistencyRequestPlus:
			consistency = view_v1.ViewQueryRequest_REQUEST_PLUS
		case ViewScanConsistencyUpdateAfter:
			consistency = view_v1.ViewQueryRequest_UPDATE_AFTER
		}
		req.ScanConsistency = &consistency
	}
	if opts.Skip > 0 {
		req.Skip = &opts.Skip
	}
	if opts.Limit > 0 {
		req.Limit = &opts.Limit
	}
	if opts.Order > 0 {
		var order view_v1.ViewQueryRequest_Order
		switch opts.Order {
		case ViewOrderingAscending:
			order = view_v1.ViewQueryRequest_ASCENDING
		case ViewOrderingDescending:
			order = view_v1.ViewQueryRequest_DESCENDING
		}

		req.Order = &order
	}
	if opts.Reduce {
		req.Reduce = &opts.Reduce
	}
	if opts.Group {
		req.Group = &opts.Group
	}
	if opts.GroupLevel > 0 {
		req.GroupLevel = &opts.GroupLevel
	}
	if opts.Key != nil {
		b, err := json.Marshal(opts.Key)
		if err != nil {
			return nil, err
		}
		req.Key = b
	}
	if opts.StartKey != nil {
		b, err := json.Marshal(opts.StartKey)
		if err != nil {
			return nil, err
		}
		req.StartKey = b
	}
	if opts.EndKey != nil {
		b, err := json.Marshal(opts.EndKey)
		if err != nil {
			return nil, err
		}
		req.EndKey = b
	}
	if opts.InclusiveEnd {
		req.InclusiveEnd = &opts.InclusiveEnd
	}
	if opts.StartKeyDocID != "" {
		req.StartKeyDocId = &opts.StartKeyDocID
	}
	if opts.EndKeyDocID != "" {
		req.EndKeyDocId = &opts.EndKeyDocID
	}
	if opts.OnError > 0 {
		var onError view_v1.ViewQueryRequest_ErrorMode
		switch opts.OnError {
		case ViewErrorModeContinue:
			onError = view_v1.ViewQueryRequest_CONTINUE
		case ViewErrorModeStop:
			onError = view_v1.ViewQueryRequest_STOP
		}

		req.OnError = &onError
	}
	if opts.Debug {
		req.Debug = &opts.Debug
	}
	if opts.Namespace > 0 {
		var namespace view_v1.ViewQueryRequest_DesignDocumentNamespace
		switch opts.Namespace {
		case DesignDocumentNamespaceProduction:
			namespace = view_v1.ViewQueryRequest_PRODUCTION
		case DesignDocumentNamespaceDevelopment:
			namespace = view_v1.ViewQueryRequest_DEVELOPMENT
		}

		req.Namespace = &namespace
	}

	res, err := b.client.viewClient.ViewQuery(ctx, req)
	if err != nil {
		return nil, err
	}

	return &ViewResult{
		client: res,
	}, nil
}
