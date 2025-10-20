package server_v1

import (
	"errors"
	"time"

	"github.com/couchbase/gocbcorex/cbsearchx"
	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/couchbase/goprotostellar/genproto/search_v1"
	"github.com/couchbase/stellar-gateway/gateway/apiversion"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SearchServer struct {
	search_v1.UnimplementedSearchServiceServer

	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
}

func NewSearchServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
) *SearchServer {
	return &SearchServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
	}
}

func (s *SearchServer) SearchQuery(in *search_v1.SearchQueryRequest, out search_v1.SearchService_SearchQueryServer) error {
	if len(in.Knn) > 0 {
		errSt := checkApiVersion(out.Context(), apiversion.VectorSearch, "Knn")
		if errSt != nil {
			return errSt.Err()
		}
	}
	if in.KnnOperator != nil {
		errSt := checkApiVersion(out.Context(), apiversion.VectorSearch, "KnnOperator")
		if errSt != nil {
			return errSt.Err()
		}
	}

	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(out.Context(), nil)
	if errSt != nil {
		return errSt.Err()
	}

	if in.Query == nil {
		return status.Errorf(codes.InvalidArgument, "query option must be specified")
	}
	if in.IndexName == "" {
		return status.Errorf(codes.InvalidArgument, "index name option must be specified")
	}

	var opts cbsearchx.QueryOptions
	opts.OnBehalfOf = oboInfo

	opts.Collections = in.Collections

	// At present protostellar only supports not bounded.
	switch in.ScanConsistency {
	case search_v1.SearchQueryRequest_SCAN_CONSISTENCY_NOT_BOUNDED:
		opts.Control = &cbsearchx.Control{
			Consistency: &cbsearchx.Consistency{
				Level: cbsearchx.ConsistencyLevelNotBounded,
			},
		}
	default:
		return status.Errorf(codes.InvalidArgument, "invalid scan consistency option specified")
	}

	opts.Explain = in.IncludeExplanation

	if len(in.Facets) > 0 {
		facets := make(map[string]cbsearchx.Facet, len(in.Facets))
		for k, facet := range in.Facets {
			switch f := facet.Facet.(type) {
			case *search_v1.Facet_TermFacet:
				facets[k] = &cbsearchx.TermFacet{
					Field: f.TermFacet.Field,
					Size:  uint64(f.TermFacet.Size),
				}
			case *search_v1.Facet_DateRangeFacet:
				dateRanges := make([]cbsearchx.DateFacetRange, len(f.DateRangeFacet.DateRanges))
				for i, dr := range f.DateRangeFacet.DateRanges {
					dateRanges[i] = cbsearchx.DateFacetRange{
						Name:  dr.Name,
						Start: dr.Start,
						End:   dr.End,
					}
				}
				facets[k] = &cbsearchx.DateFacet{
					Field:      f.DateRangeFacet.Field,
					Size:       uint64(f.DateRangeFacet.Size),
					DateRanges: dateRanges,
				}
			case *search_v1.Facet_NumericRangeFacet:
				numericRanges := make([]cbsearchx.NumericFacetRange, len(f.NumericRangeFacet.NumericRanges))
				for i, dr := range f.NumericRangeFacet.NumericRanges {
					numericRanges[i] = cbsearchx.NumericFacetRange{
						Name: dr.Name,
					}
					if dr.Min != nil {
						min := float64(dr.GetMin())
						numericRanges[i].Min = &min
					}
					if dr.Max != nil {
						max := float64(dr.GetMax())
						numericRanges[i].Max = &max
					}
				}
				facets[k] = &cbsearchx.NumericFacet{
					Field:         f.NumericRangeFacet.Field,
					Size:          uint64(f.NumericRangeFacet.Size),
					NumericRanges: numericRanges,
				}
			}
		}

		opts.Facets = facets
	}

	opts.Fields = in.Fields

	opts.From = int(in.Skip)

	opts.Highlight = &cbsearchx.Highlight{}
	switch in.HighlightStyle {
	case search_v1.SearchQueryRequest_HIGHLIGHT_STYLE_DEFAULT:
		opts.Highlight.Style = cbsearchx.DefaultHighlightStyle
	case search_v1.SearchQueryRequest_HIGHLIGHT_STYLE_ANSI:
		opts.Highlight.Style = cbsearchx.AnsiHightlightStyle
	case search_v1.SearchQueryRequest_HIGHLIGHT_STYLE_HTML:
		opts.Highlight.Style = cbsearchx.HTMLHighlightStyle
	default:
		return status.Errorf(codes.InvalidArgument, "invalid highlight style option specified")
	}
	opts.Highlight.Fields = in.HighlightFields

	opts.IncludeLocations = in.IncludeLocations

	if len(in.Knn) > 0 {
		knns := make([]cbsearchx.KnnQuery, len(in.Knn))
		for i, knnQuery := range in.Knn {
			knns[i] = cbsearchx.KnnQuery{
				Boost:  ptr.Deref(knnQuery.Boost, 0),
				Field:  knnQuery.Field,
				K:      knnQuery.K,
				Vector: knnQuery.Vector,
			}
		}

		opts.Knn = knns
	}
	if in.KnnOperator != nil {
		knnOperator, errSt := knnOperatorToCbsearchx(*in.KnnOperator)
		if errSt != nil {
			return errSt.Err()
		}

		opts.KnnOperator = knnOperator
	}

	var err error
	opts.Query, err = s.translatePSQueryToCBSearchX(in.Query)
	if err != nil {
		return err
	}

	if in.DisableScoring {
		opts.Score = "none"
	}

	// At present protostellar does not support..
	// opts.SearchAfter
	// opts.SearchBefore

	opts.Size = int(in.Limit)

	opts.Sort, err = s.translatePSSortToCBSearchX(in.Sort)
	if err != nil {
		return err
	}

	opts.IndexName = in.IndexName

	result, err := agent.Search(out.Context(), &opts)
	if err != nil {
		if errors.Is(err, cbsearchx.ErrIndexNotFound) {
			return s.errorHandler.NewSearchIndexMissingStatus(err, in.IndexName).Err()
		} else if errors.Is(err, cbsearchx.ErrIndexNameInvalid) {
			return s.errorHandler.NewSearchIndexNameInvalidStatus(err, in.IndexName).Err()
		}
		return s.errorHandler.NewGenericStatus(err).Err()
	}

	var rowCache []*search_v1.SearchQueryResponse_SearchQueryRow
	rowCacheNumBytes := 0
	const MaxRowBytes = 1024

	for result.HasMoreHits() {
		row, err := result.ReadHit()
		if err != nil {
			return s.errorHandler.NewGenericStatus(err).Err()
		}

		fragments := make(map[string]*search_v1.SearchQueryResponse_Fragment, len(row.Fragments))
		for k, v := range row.Fragments {
			fragments[k] = &search_v1.SearchQueryResponse_Fragment{
				Content: v,
			}
		}

		// Go won't let us use map[string]json.RawMessage as map[string][]byte.
		fields := make(map[string][]byte, len(row.Fields))
		for k, v := range row.Fields {
			fields[k] = v
		}

		var locations []*search_v1.SearchQueryResponse_Location
		for field, terms := range row.Locations {
			for term, locs := range terms {
				for _, location := range locs {
					locations = append(locations, &search_v1.SearchQueryResponse_Location{
						Field:          field,
						Term:           term,
						Position:       location.Position,
						Start:          location.Start,
						End:            location.End,
						ArrayPositions: location.ArrayPositions,
					})
				}
			}
		}

		psRow := &search_v1.SearchQueryResponse_SearchQueryRow{
			Id:          row.ID,
			Score:       row.Score,
			Index:       row.Index,
			Explanation: row.Explanation,
			Locations:   locations,
			Fields:      fields,
			Fragments:   fragments,
		}

		psRowSize := proto.Size(psRow)
		if rowCacheNumBytes+psRowSize > MaxRowBytes {
			err := out.Send(&search_v1.SearchQueryResponse{
				Hits: rowCache,
			})
			if err != nil {
				return s.errorHandler.NewGenericStatus(err).Err()
			}
			rowCache = nil
			rowCacheNumBytes = 0
		}

		rowCache = append(rowCache, psRow)
		rowCacheNumBytes += psRowSize
	}

	facetsPs := make(map[string]*search_v1.SearchQueryResponse_FacetResult)
	facets, err := result.Facets()
	if err == nil {
		for name, facet := range facets {
			if len(facet.Terms) > 0 {
				terms := make([]*search_v1.SearchQueryResponse_TermResult, len(facet.Terms))
				for i, term := range facet.Terms {
					terms[i] = &search_v1.SearchQueryResponse_TermResult{
						Name: term.Term,
						// Field: ,
						Size: uint64(term.Count),
					}
				}
				f := &search_v1.SearchQueryResponse_FacetResult_TermFacet{
					TermFacet: &search_v1.SearchQueryResponse_TermFacetResult{
						Field:   facet.Field,
						Total:   int64(facet.Total),
						Missing: int64(facet.Missing),
						Other:   int64(facet.Other),
						Terms:   terms,
					},
				}
				facetsPs[name] = &search_v1.SearchQueryResponse_FacetResult{
					SearchFacet: f,
				}
			} else if len(facet.NumericRanges) > 0 {
				nRanges := make([]*search_v1.SearchQueryResponse_NumericRangeResult, len(facet.NumericRanges))
				for i, r := range facet.NumericRanges {
					nRanges[i] = &search_v1.SearchQueryResponse_NumericRangeResult{
						Name: r.Name,
						// Field: "",
						Size: uint64(r.Count),
						Min:  uint64(r.Min),
						Max:  uint64(r.Max),
					}
				}
				f := &search_v1.SearchQueryResponse_FacetResult_NumericRangeFacet{
					NumericRangeFacet: &search_v1.SearchQueryResponse_NumericRangeFacetResult{
						Field:         facet.Field,
						Total:         int64(facet.Total),
						Missing:       int64(facet.Missing),
						Other:         int64(facet.Other),
						NumericRanges: nRanges,
					},
				}
				facetsPs[name] = &search_v1.SearchQueryResponse_FacetResult{
					SearchFacet: f,
				}
			} else if len(facet.DateRanges) > 0 {
				nRanges := make([]*search_v1.SearchQueryResponse_DateRangeResult, len(facet.DateRanges))
				for i, r := range facet.DateRanges {
					var start *timestamppb.Timestamp
					var end *timestamppb.Timestamp
					if r.Start != "" {
						t, err := time.Parse(time.RFC3339, r.Start)
						if err != nil {
							return err
						}
						start = timestamppb.New(t)
					}
					if r.End != "" {
						t, err := time.Parse(time.RFC3339, r.End)
						if err != nil {
							return err
						}
						end = timestamppb.New(t)
					}
					nRanges[i] = &search_v1.SearchQueryResponse_DateRangeResult{
						Name: r.Name,
						// Field: "",
						Size:  uint64(r.Count),
						Start: start,
						End:   end,
					}
				}
				f := &search_v1.SearchQueryResponse_FacetResult_DateRangeFacet{
					DateRangeFacet: &search_v1.SearchQueryResponse_DateRangeFacetResult{
						Field:      facet.Field,
						Total:      int64(facet.Total),
						Missing:    int64(facet.Missing),
						Other:      int64(facet.Other),
						DateRanges: nRanges,
					},
				}
				facetsPs[name] = &search_v1.SearchQueryResponse_FacetResult{
					SearchFacet: f,
				}
			}
		}
	}

	var metadataPs *search_v1.SearchQueryResponse_MetaData
	metaData, err := result.MetaData()
	if err == nil {
		metadataPs = &search_v1.SearchQueryResponse_MetaData{
			Metrics: &search_v1.SearchQueryResponse_SearchMetrics{
				ExecutionTime:         durationFromGo(metaData.Metrics.Took),
				TotalRows:             metaData.Metrics.TotalHits,
				MaxScore:              metaData.Metrics.MaxScore,
				TotalPartitionCount:   metaData.Metrics.TotalPartitionCount,
				SuccessPartitionCount: metaData.Metrics.SuccessfulPartitionCount,
				ErrorPartitionCount:   metaData.Metrics.FailedPartitionCount,
			},
			Errors: metaData.Errors,
		}
	}

	// if we have any rows, meta-data, or facets left to stream, we send that first
	// before we process any errors that occurred.
	if len(rowCache) > 0 || metadataPs != nil || len(facetsPs) > 0 {
		err := out.Send(&search_v1.SearchQueryResponse{
			Hits:     rowCache,
			MetaData: metadataPs,
			Facets:   facetsPs,
		})
		if err != nil {
			return s.errorHandler.NewGenericStatus(err).Err()
		}
	}

	return nil
}

func (s *SearchServer) translatePSSortToCBSearchX(in []*search_v1.Sorting) ([]cbsearchx.Sort, error) {
	sorts := make([]cbsearchx.Sort, len(in))
	for i, sorting := range in {
		switch s := sorting.Sorting.(type) {
		case *search_v1.Sorting_FieldSorting:
			sorts[i] = &cbsearchx.SortField{
				Descending: &s.FieldSorting.Descending,
				Field:      s.FieldSorting.GetField(),
				Missing:    s.FieldSorting.Missing,
				Mode:       s.FieldSorting.Mode,
				Type:       s.FieldSorting.Type,
			}
		case *search_v1.Sorting_GeoDistanceSorting:
			thisSort := &cbsearchx.SortGeoDistance{
				Descending: &s.GeoDistanceSorting.Descending,
				Field:      s.GeoDistanceSorting.GetField(),
				Unit:       s.GeoDistanceSorting.Unit,
			}

			if s.GeoDistanceSorting.Center != nil {
				thisSort.Location = &cbsearchx.Location{
					Lat: s.GeoDistanceSorting.Center.Latitude,
					Lon: s.GeoDistanceSorting.Center.Longitude,
				}
			}
			sorts[i] = thisSort
		case *search_v1.Sorting_IdSorting:
			sorts[i] = &cbsearchx.SortID{
				Descending: &s.IdSorting.Descending,
			}
		case *search_v1.Sorting_ScoreSorting:
			sorts[i] = &cbsearchx.SortScore{
				Descending: &s.ScoreSorting.Descending,
			}
		default:
			return nil, status.Errorf(codes.InvalidArgument, "invalid sort option specified")
		}
	}

	return sorts, nil
}

func (s *SearchServer) translatePSQueryToCBSearchX(in *search_v1.Query) (cbsearchx.Query, error) {
	switch query := in.Query.(type) {
	case *search_v1.Query_BooleanFieldQuery:
		return &cbsearchx.BooleanFieldQuery{
			Bool:  query.BooleanFieldQuery.Value,
			Boost: query.BooleanFieldQuery.GetBoost(),
			Field: query.BooleanFieldQuery.GetField(),
		}, nil
	case *search_v1.Query_BooleanQuery:
		var must *cbsearchx.ConjunctionQuery
		if query.BooleanQuery.Must != nil {
			must = &cbsearchx.ConjunctionQuery{}
			must.Boost = query.BooleanQuery.Must.GetBoost()
			queries := make([]cbsearchx.Query, len(query.BooleanQuery.Must.Queries))
			for i, thisQ := range query.BooleanQuery.Must.Queries {
				q, err := s.translatePSQueryToCBSearchX(thisQ)
				if err != nil {
					return nil, err
				}

				queries[i] = q
			}
			must.Conjuncts = queries
		}
		var mustNot *cbsearchx.DisjunctionQuery
		if query.BooleanQuery.MustNot != nil {
			mustNot = &cbsearchx.DisjunctionQuery{}
			mustNot.Boost = query.BooleanQuery.MustNot.GetBoost()
			mustNot.Min = query.BooleanQuery.MustNot.GetMinimum()

			queries := make([]cbsearchx.Query, len(query.BooleanQuery.MustNot.Queries))
			for i, thisQ := range query.BooleanQuery.MustNot.Queries {
				q, err := s.translatePSQueryToCBSearchX(thisQ)
				if err != nil {
					return nil, err
				}

				queries[i] = q
			}
			mustNot.Disjuncts = queries
		}
		var should *cbsearchx.DisjunctionQuery
		if query.BooleanQuery.Should != nil {
			should = &cbsearchx.DisjunctionQuery{}
			should.Boost = query.BooleanQuery.Should.GetBoost()
			should.Min = query.BooleanQuery.Should.GetMinimum()

			queries := make([]cbsearchx.Query, len(query.BooleanQuery.Should.Queries))
			for i, thisQ := range query.BooleanQuery.Should.Queries {
				q, err := s.translatePSQueryToCBSearchX(thisQ)
				if err != nil {
					return nil, err
				}

				queries[i] = q
			}
			should.Disjuncts = queries
		}
		return &cbsearchx.BooleanQuery{
			Boost:   query.BooleanQuery.GetBoost(),
			Must:    must,
			MustNot: mustNot,
			Should:  should,
		}, nil
	case *search_v1.Query_ConjunctionQuery:
		queries := make([]cbsearchx.Query, len(query.ConjunctionQuery.Queries))
		var err error
		for i, q := range query.ConjunctionQuery.Queries {
			queries[i], err = s.translatePSQueryToCBSearchX(q)
			if err != nil {
				return nil, err
			}
		}
		return &cbsearchx.ConjunctionQuery{
			Boost:     query.ConjunctionQuery.GetBoost(),
			Conjuncts: queries,
		}, nil
	case *search_v1.Query_DateRangeQuery:
		return &cbsearchx.DateRangeQuery{
			Boost:          query.DateRangeQuery.GetBoost(),
			DateTimeParser: query.DateRangeQuery.GetDateTimeParser(),
			End:            query.DateRangeQuery.GetEndDate(),
			Field:          query.DateRangeQuery.GetField(),
			// InclusiveStart: query.DateRangeQuery,
			// InclusiveEnd:   false,
			Start: query.DateRangeQuery.GetStartDate(),
		}, nil
	case *search_v1.Query_DisjunctionQuery:
		queries := make([]cbsearchx.Query, len(query.DisjunctionQuery.Queries))
		var err error
		for i, q := range query.DisjunctionQuery.Queries {
			queries[i], err = s.translatePSQueryToCBSearchX(q)
			if err != nil {
				return nil, err
			}
		}
		return &cbsearchx.DisjunctionQuery{
			Boost:     query.DisjunctionQuery.GetBoost(),
			Disjuncts: queries,
			Min:       query.DisjunctionQuery.GetMinimum(),
		}, nil
	case *search_v1.Query_DocIdQuery:
		return &cbsearchx.DocIDQuery{
			Boost:  query.DocIdQuery.GetBoost(),
			DocIds: query.DocIdQuery.GetIds(),
		}, nil
	case *search_v1.Query_GeoBoundingBoxQuery:
		return &cbsearchx.GeoBoundingBoxQuery{
			BottomRight: cbsearchx.Location{
				Lat: query.GeoBoundingBoxQuery.BottomRight.Latitude,
				Lon: query.GeoBoundingBoxQuery.BottomRight.Longitude,
			},
			Boost: query.GeoBoundingBoxQuery.GetBoost(),
			Field: query.GeoBoundingBoxQuery.GetField(),
			TopLeft: cbsearchx.Location{
				Lat: query.GeoBoundingBoxQuery.TopLeft.Latitude,
				Lon: query.GeoBoundingBoxQuery.TopLeft.Longitude,
			},
		}, nil
	case *search_v1.Query_GeoDistanceQuery:
		return &cbsearchx.GeoDistanceQuery{
			Distance: query.GeoDistanceQuery.Distance,
			Boost:    query.GeoDistanceQuery.GetBoost(),
			Field:    query.GeoDistanceQuery.GetField(),
			Location: cbsearchx.Location{
				Lat: query.GeoDistanceQuery.Center.Latitude,
				Lon: query.GeoDistanceQuery.Center.Longitude,
			},
		}, nil
	case *search_v1.Query_GeoPolygonQuery:
		points := make([]cbsearchx.Location, len(query.GeoPolygonQuery.Vertices))
		for i, p := range query.GeoPolygonQuery.Vertices {
			points[i] = cbsearchx.Location{
				Lat: p.Latitude,
				Lon: p.Longitude,
			}
		}
		return &cbsearchx.GeoPolygonQuery{
			Boost:         query.GeoPolygonQuery.GetBoost(),
			Field:         query.GeoPolygonQuery.GetField(),
			PolygonPoints: points,
		}, nil
	case *search_v1.Query_MatchAllQuery:
		return &cbsearchx.MatchAllQuery{}, nil
	case *search_v1.Query_MatchNoneQuery:
		return &cbsearchx.MatchNoneQuery{}, nil
	case *search_v1.Query_MatchPhraseQuery:
		return &cbsearchx.MatchPhraseQuery{
			Analyzer: query.MatchPhraseQuery.GetAnalyzer(),
			Boost:    query.MatchPhraseQuery.GetBoost(),
			Field:    query.MatchPhraseQuery.GetField(),
			Phrase:   query.MatchPhraseQuery.Phrase,
		}, nil
	case *search_v1.Query_MatchQuery:
		q := &cbsearchx.MatchQuery{
			Analyzer:     query.MatchQuery.GetAnalyzer(),
			Boost:        query.MatchQuery.GetBoost(),
			Field:        query.MatchQuery.GetField(),
			Fuzziness:    query.MatchQuery.GetFuzziness(),
			Match:        query.MatchQuery.Value,
			PrefixLength: query.MatchQuery.GetPrefixLength(),
		}
		if query.MatchQuery.Operator != nil {
			switch *query.MatchQuery.Operator {
			case search_v1.MatchQuery_OPERATOR_OR:
				q.Operator = cbsearchx.MatchOperatorOr
			case search_v1.MatchQuery_OPERATOR_AND:
				q.Operator = cbsearchx.MatchOperatorAnd
			default:
				return nil, status.Errorf(codes.InvalidArgument, "invalid match operation option specified")
			}
		}
		return q, nil
	case *search_v1.Query_NumericRangeQuery:
		return &cbsearchx.NumericRangeQuery{
			Boost:        query.NumericRangeQuery.GetBoost(),
			Field:        query.NumericRangeQuery.GetField(),
			InclusiveMin: query.NumericRangeQuery.GetInclusiveMin(),
			InclusiveMax: query.NumericRangeQuery.GetInclusiveMax(),
			Min:          query.NumericRangeQuery.GetMin(),
			Max:          query.NumericRangeQuery.GetMax(),
		}, nil
	case *search_v1.Query_PhraseQuery:
		return &cbsearchx.PhraseQuery{
			Boost: query.PhraseQuery.GetBoost(),
			Field: query.PhraseQuery.GetField(),
			Terms: query.PhraseQuery.Terms,
		}, nil
	case *search_v1.Query_PrefixQuery:
		return &cbsearchx.PrefixQuery{
			Boost:  query.PrefixQuery.GetBoost(),
			Field:  query.PrefixQuery.GetField(),
			Prefix: query.PrefixQuery.Prefix,
		}, nil
	case *search_v1.Query_QueryStringQuery:
		return &cbsearchx.QueryStringQuery{
			Boost: query.QueryStringQuery.GetBoost(),
			Query: query.QueryStringQuery.QueryString,
		}, nil
	case *search_v1.Query_RegexpQuery:
		return &cbsearchx.RegexpQuery{
			Boost:  query.RegexpQuery.GetBoost(),
			Field:  query.RegexpQuery.GetField(),
			Regexp: query.RegexpQuery.Regexp,
		}, nil
	case *search_v1.Query_TermQuery:
		return &cbsearchx.TermQuery{
			Boost:        query.TermQuery.GetBoost(),
			Field:        query.TermQuery.GetField(),
			Fuzziness:    query.TermQuery.GetFuzziness(),
			PrefixLength: query.TermQuery.GetPrefixLength(),
			Term:         query.TermQuery.Term,
		}, nil
	case *search_v1.Query_TermRangeQuery:
		return &cbsearchx.TermRangeQuery{
			Boost:        query.TermRangeQuery.GetBoost(),
			Field:        query.TermRangeQuery.GetField(),
			InclusiveMax: query.TermRangeQuery.GetInclusiveMax(),
			InclusiveMin: query.TermRangeQuery.GetInclusiveMin(),
			Max:          query.TermRangeQuery.GetMax(),
			Min:          query.TermRangeQuery.GetMin(),
		}, nil
	case *search_v1.Query_WildcardQuery:
		return &cbsearchx.WildcardQuery{
			Boost:    query.WildcardQuery.GetBoost(),
			Field:    query.WildcardQuery.GetField(),
			Wildcard: query.WildcardQuery.Wildcard,
		}, nil
	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid query option specified")
	}
}
