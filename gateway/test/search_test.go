package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/couchbase/goprotostellar/genproto/admin_search_v1"
	"github.com/couchbase/goprotostellar/genproto/search_v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *GatewayOpsTestSuite) TestSearchBasic() {
	if !s.SupportsFeature(TestFeatureSearch) {
		s.T().Skip()
	}
	helper := &testSearchServiceHelper{
		GatewayOpsTestSuite: s,
	}

	if s.Run("SearchSetup", helper.testSetupSearch) {

		s.Run("Test", helper.TestSearchBasic)

		s.Run("TestBadCredentials", helper.TestSearchBadCredentials)

		s.Run("TestInsufficientPermissions", helper.TestSearchInsufficientPermissions)

		s.Run("TestQueryStringQuery", helper.TestQueryStringQuery)

		s.Run("TestRegExpQuery", helper.TestRegExpQuery)

		s.Run("TestPrefixQuery", helper.TestPrefixQuery)

		s.Run("TestTermQuery", helper.TestTermQuery)

		s.Run("TestTermRangeQuery", helper.TestTermRangeQuery)

		s.Run("TestPhraseQuery", helper.TestPhraseQuery)

		s.Run("TestWildcardQuery", helper.TestWildcardQuery)

		s.Run("TestDocIDQuery", helper.TestDocIDQuery)

		s.Run("TestMatchQuery", helper.TestMatchQuery)

		s.Run("TestMatchPhraseQuery", helper.TestMatchPhraseQuery)

		s.Run("TestMatchAllQuery", helper.TestMatchAllQuery)

		s.Run("TestMatchNoneQuery", helper.TestMatchNoneQuery)

		s.Run("TestNumericRangeQuery", helper.TestNumericRangeQuery)

		s.Run("TestDisjunctionQuery", helper.TestDisjunctionQuery)

		s.Run("TestBooleanQuery", helper.TestBooleanQuery)

		s.Run("TestBooleanFieldQuery", helper.TestBooleanFieldQuery)

		s.Run("TestDateRangeQuery", helper.TestDateRangeQuery)

		s.Run("TestGeoDistanceQuery", helper.TestGeoDistanceQuery)

		s.Run("TestGeoBoundingBoxQuery", helper.TestGeoBoundingBoxQuery)

		s.Run("TestGeoPolygonQuery", helper.TestGeoPolygonQuery)

		s.Run("Cleanup", helper.testCleanupSearch)

	}
}

func (s *testSearchServiceHelper) validateNumResults(result search_v1.SearchService_SearchQueryClient, expected int) {
	res, err := result.Recv()
	s.Require().NoError(err, "Failed to read row")
	s.Require().Equal(expected, len(res.Hits))
}

type testSearchServiceHelper struct {
	*GatewayOpsTestSuite
	IndexName   string
	Dataset     []testBreweryDocumentJson
	IndexClient admin_search_v1.SearchAdminServiceClient
}

func (s *testSearchServiceHelper) TestQueryStringQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	query := &search_v1.Query_QueryStringQuery{
		QueryStringQuery: &search_v1.QueryStringQuery{
			QueryString: "microbrewery",
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 1)
}

func (s *testSearchServiceHelper) TestRegExpQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	field := "description"
	query := &search_v1.Query_RegexpQuery{
		RegexpQuery: &search_v1.RegexpQuery{
			Regexp: ".*baseball*.",
			Field:  &field,
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 1)
}

func (s *testSearchServiceHelper) TestPrefixQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	field := "type"
	query := &search_v1.Query_PrefixQuery{
		PrefixQuery: &search_v1.PrefixQuery{
			Prefix: "brew",
			Field:  &field,
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, len(s.Dataset))
}

func (s *testSearchServiceHelper) TestTermQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	field := "description"
	query := &search_v1.Query_TermQuery{
		TermQuery: &search_v1.TermQuery{
			Term:  "brews",
			Field: &field,
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 2)
}

func (s *testSearchServiceHelper) TestTermRangeQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	min := "brew"
	max := "brewery"
	trueBool := true
	falseBool := false
	query := &search_v1.Query_TermRangeQuery{
		TermRangeQuery: &search_v1.TermRangeQuery{
			Min:          &min,
			Max:          &max,
			InclusiveMin: &trueBool,
			InclusiveMax: &falseBool,
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 1)
}

func (s *testSearchServiceHelper) TestPhraseQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	field := "description"
	query := &search_v1.Query_PhraseQuery{
		PhraseQuery: &search_v1.PhraseQuery{
			Terms: []string{"grilled", "cuisine"},
			Field: &field,
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 1)
}

func (s *testSearchServiceHelper) TestWildcardQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	query := &search_v1.Query_WildcardQuery{
		WildcardQuery: &search_v1.WildcardQuery{
			Wildcard: "win*",
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 1)
}

func (s *testSearchServiceHelper) TestDocIDQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)

	var ids []string
	for _, doc := range s.Dataset {
		ids = append(ids, fmt.Sprintf("search-%s", doc.Name))
	}

	query := &search_v1.Query_DocIdQuery{
		DocIdQuery: &search_v1.DocIdQuery{
			Ids: ids,
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, len(s.Dataset))
}

func (s *testSearchServiceHelper) TestMatchQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	query := &search_v1.Query_MatchQuery{
		MatchQuery: &search_v1.MatchQuery{
			Value: "loft",
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 1)
}

func (s *testSearchServiceHelper) TestMatchPhraseQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	query := &search_v1.Query_MatchPhraseQuery{
		MatchPhraseQuery: &search_v1.MatchPhraseQuery{
			Phrase: "grilled cuisine",
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 1)
}

// What is the point in this?
func (s *testSearchServiceHelper) TestMatchAllQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)

	matchQuery := &search_v1.Query_MatchAllQuery{
		MatchAllQuery: &search_v1.MatchAllQuery{},
	}

	var ids []string
	for _, doc := range s.Dataset {
		ids = append(ids, fmt.Sprintf("search-%s", doc.Name))
	}

	docIDQuery := &search_v1.Query_DocIdQuery{
		DocIdQuery: &search_v1.DocIdQuery{
			Ids: ids,
		},
	}

	// There are docs under this index other than those inserted as part of this test. To avoid flakiness caused by
	// other tests adding/removing docs we use the DocIDQuery to limit the return of MatchAll to those in the dataset
	query := &search_v1.Query_ConjunctionQuery{
		ConjunctionQuery: &search_v1.ConjunctionQuery{
			Queries: []*search_v1.Query{
				{
					Query: matchQuery,
				},
				{
					Query: docIDQuery,
				},
			},
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, len(s.Dataset))
}

func (s *testSearchServiceHelper) TestMatchNoneQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	query := &search_v1.Query_MatchNoneQuery{
		MatchNoneQuery: &search_v1.MatchNoneQuery{},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 0)
}

func (s *testSearchServiceHelper) TestNumericRangeQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	var min, max float32
	field := "capacity"
	min = 50
	max = 150
	query := &search_v1.Query_NumericRangeQuery{
		NumericRangeQuery: &search_v1.NumericRangeQuery{
			Min:   &min,
			Max:   &max,
			Field: &field,
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 2)
}

func (s *testSearchServiceHelper) TestDisjunctionQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)

	texasQuery := &search_v1.Query_QueryStringQuery{
		QueryStringQuery: &search_v1.QueryStringQuery{
			QueryString: "Texas",
		},
	}

	sanFranQuery := &search_v1.Query_QueryStringQuery{
		QueryStringQuery: &search_v1.QueryStringQuery{
			QueryString: "San Francisco",
		},
	}

	query := &search_v1.Query_DisjunctionQuery{
		DisjunctionQuery: &search_v1.DisjunctionQuery{
			Queries: []*search_v1.Query{
				{
					Query: texasQuery,
				},
				{
					Query: sanFranQuery,
				},
			},
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 2)
}

func (s *testSearchServiceHelper) TestBooleanQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)

	beerQuery := &search_v1.Query_QueryStringQuery{
		QueryStringQuery: &search_v1.QueryStringQuery{
			QueryString: "beer",
		},
	}

	softQuery := &search_v1.Query_QueryStringQuery{
		QueryStringQuery: &search_v1.QueryStringQuery{
			QueryString: "soft",
		},
	}

	awardQuery := &search_v1.Query_QueryStringQuery{
		QueryStringQuery: &search_v1.QueryStringQuery{
			QueryString: "award",
		},
	}

	must := &search_v1.ConjunctionQuery{
		Queries: []*search_v1.Query{
			{
				Query: beerQuery,
			},
		},
	}

	mustNot := &search_v1.DisjunctionQuery{
		Queries: []*search_v1.Query{
			{
				Query: softQuery,
			},
		},
	}

	should := &search_v1.DisjunctionQuery{
		Queries: []*search_v1.Query{
			{
				Query: awardQuery,
			},
		},
	}

	query := &search_v1.Query_BooleanQuery{
		BooleanQuery: &search_v1.BooleanQuery{
			Must:    must,
			MustNot: mustNot,
			Should:  should,
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 1)
}

func (s *testSearchServiceHelper) TestBooleanFieldQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	field := "dogFriendly"

	query := &search_v1.Query_BooleanFieldQuery{
		BooleanFieldQuery: &search_v1.BooleanFieldQuery{
			Field: &field,
			Value: true,
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	result, err := queryResult.Recv()
	s.Require().NoError(err, "Failed to read row")
	s.Require().Equal(4, len(result.Hits))
}

func (s *testSearchServiceHelper) TestDateRangeQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	start := "1970-01-01"
	end := "2001-01-01"

	query := &search_v1.Query_DateRangeQuery{
		DateRangeQuery: &search_v1.DateRangeQuery{
			StartDate: &start,
			EndDate:   &end,
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 3)
}

func (s *testSearchServiceHelper) TestGeoDistanceQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	field := "geo"
	query := &search_v1.Query_GeoDistanceQuery{
		GeoDistanceQuery: &search_v1.GeoDistanceQuery{
			Center: &search_v1.LatLng{
				Latitude:  51,
				Longitude: 6,
			},
			Distance: "110km",
			Field:    &field,
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 1)
}

func (s *testSearchServiceHelper) TestGeoBoundingBoxQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	query := &search_v1.Query_GeoBoundingBoxQuery{
		GeoBoundingBoxQuery: &search_v1.GeoBoundingBoxQuery{
			TopLeft: &search_v1.LatLng{
				Latitude:  38,
				Longitude: -123,
			},
			BottomRight: &search_v1.LatLng{
				Latitude:  30,
				Longitude: -96,
			},
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 2)
}

func (s *testSearchServiceHelper) TestGeoPolygonQuery() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)
	query := &search_v1.Query_GeoPolygonQuery{
		GeoPolygonQuery: &search_v1.GeoPolygonQuery{
			Vertices: []*search_v1.LatLng{
				{
					Latitude:  38,
					Longitude: -122.393,
				},
				{
					Latitude:  37,
					Longitude: -123.393,
				},
				{
					Latitude:  37,
					Longitude: -121.393,
				},
			},
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	s.Require().NoError(err, "Failed to query index")
	s.validateNumResults(queryResult, 1)
}

func (s *testSearchServiceHelper) TestSearchBasic() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)

	field := "service"
	query := &search_v1.Query_TermQuery{
		TermQuery: &search_v1.TermQuery{
			Term:  "search",
			Field: &field,
		},
	}
	var result *search_v1.SearchQueryResponse
	var rows []*search_v1.SearchQueryResponse_SearchQueryRow
	s.Require().Eventually(func() bool {
		var err error
		var min float32 = 0
		var max float32 = 501
		start := "2000-07-22 20:00:20"
		end := "2020-07-22 20:00:20"
		queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
			IndexName: s.IndexName,
			Query: &search_v1.Query{
				Query: query,
			},
			Facets: map[string]*search_v1.Facet{
				"type": {
					Facet: &search_v1.Facet_TermFacet{
						TermFacet: &search_v1.TermFacet{Field: "country", Size: 5},
					},
				},
				"date": {
					Facet: &search_v1.Facet_DateRangeFacet{
						DateRangeFacet: &search_v1.DateRangeFacet{
							Field: "updated",
							Size:  5,
							DateRanges: []*search_v1.DateRange{
								{
									Name:  "updated",
									Start: &start,
									End:   &end,
								},
							},
						},
					},
				},
				"numeric": {
					Facet: &search_v1.Facet_NumericRangeFacet{
						NumericRangeFacet: &search_v1.NumericRangeFacet{
							Field: "capacity",
							Size:  5,
							NumericRanges: []*search_v1.NumericRange{
								{
									Name: "capacity",
									Min:  &min,
									Max:  &max,
								},
							},
						},
					},
				},
			},
			IncludeLocations: true,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		if err != nil {
			s.T().Logf("Failed to query index: %s", err)
			return false
		}

		result, err = queryResult.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				s.T().Logf("Failed to read row: %s", err)
				return false
			}
		}

		if len(s.Dataset) == len(result.Hits) {
			rows = result.Hits
			return true
		}

		s.T().Logf("Incorrect number of rows, expected: %d, was %d", len(s.Dataset), len(result.Hits))
		return false
	}, 120*time.Second, 500*time.Millisecond)

	for _, row := range rows {
		if s.Greater(len(row.Locations), 0) {
			firstLoc := row.Locations[0]
			s.Equal("search", firstLoc.Term)
			s.Equal("service", firstLoc.Field)
			s.Zero(firstLoc.Start)
			s.NotZero(firstLoc.End)
			s.Nil(firstLoc.ArrayPositions)
		}
	}

	s.NotZero(result.MetaData.Metrics.TotalRows)
	s.NotZero(result.MetaData.Metrics.ExecutionTime)
	s.NotZero(result.MetaData.Metrics.MaxScore)
	s.Zero(result.MetaData.Metrics.ErrorPartitionCount)
	s.NotZero(result.MetaData.Metrics.SuccessPartitionCount)
	s.NotZero(result.MetaData.Metrics.TotalPartitionCount)

	if s.Contains(result.Facets, "type") {
		facet := result.Facets["type"]
		f, ok := facet.SearchFacet.(*search_v1.SearchQueryResponse_FacetResult_TermFacet)
		if s.True(ok, "type facet was not a term facet result type") {
			s.Equal("country", f.TermFacet.Field)
			s.Equal(int64(7), f.TermFacet.Total)
			s.Equal(4, len(f.TermFacet.Terms))
			for _, term := range f.TermFacet.Terms {
				switch term.Name {
				case "belgium":
					s.Equal(uint64(2), term.Size)
				case "states":
					s.Equal(uint64(2), term.Size)
				case "united":
					s.Equal(uint64(2), term.Size)
				case "norway":
					s.Equal(uint64(1), term.Size)
				default:
					s.T().Fatalf("Unexpected facet term %s", term.Name)
				}
			}
		}
	}

	if s.Contains(result.Facets, "date") {
		facet := result.Facets["date"]
		f, ok := facet.SearchFacet.(*search_v1.SearchQueryResponse_FacetResult_DateRangeFacet)
		if s.True(ok, "type facet was not a daterange facet result type") {
			start, _ := time.Parse(time.RFC3339, "2000-07-22T20:00:20Z")
			end, _ := time.Parse(time.RFC3339, "2020-07-22T20:00:20Z")
			s.Equal(int64(5), f.DateRangeFacet.Total)
			s.Equal("updated", f.DateRangeFacet.Field)
			s.Equal(1, len(f.DateRangeFacet.DateRanges))
			s.Equal(uint64(5), f.DateRangeFacet.DateRanges[0].Size)
			s.Equal(start, f.DateRangeFacet.DateRanges[0].Start.AsTime())
			s.Equal(end, f.DateRangeFacet.DateRanges[0].End.AsTime())
			s.Equal("updated", f.DateRangeFacet.DateRanges[0].Name)
		}
	}

	if s.Contains(result.Facets, "numeric") {
		facet := result.Facets["numeric"]
		f, ok := facet.SearchFacet.(*search_v1.SearchQueryResponse_FacetResult_NumericRangeFacet)
		if s.True(ok, "type facet was not a numericrange facet result type") {
			s.Equal(int64(4), f.NumericRangeFacet.Total)
			s.Equal("capacity", f.NumericRangeFacet.Field)
			s.Equal(1, len(f.NumericRangeFacet.NumericRanges))
			s.Equal(uint64(4), f.NumericRangeFacet.NumericRanges[0].Size)
			s.Equal(uint64(0), f.NumericRangeFacet.NumericRanges[0].Min)
			s.Equal(uint64(501), f.NumericRangeFacet.NumericRanges[0].Max)
			s.Equal("capacity", f.NumericRangeFacet.NumericRanges[0].Name)
		}
	}
}

func (s *testSearchServiceHelper) TestSearchBadCredentials() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)

	field := "service"
	query := &search_v1.Query_TermQuery{
		TermQuery: &search_v1.TermQuery{
			Term:  "search",
			Field: &field,
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.badRpcCreds))
	requireRpcSuccess(s.T(), client, err)

	_, err = queryResult.Recv()
	assertRpcStatus(s.T(), err, codes.PermissionDenied)
	assert.Contains(s.T(), err.Error(), "No permissions to query documents.")
}

func (s *testSearchServiceHelper) TestSearchInsufficientPermissions() {
	client := search_v1.NewSearchServiceClient(s.gatewayConn)

	field := "service"
	query := &search_v1.Query_TermQuery{
		TermQuery: &search_v1.TermQuery{
			Term:  "search",
			Field: &field,
		},
	}

	queryResult, err := client.SearchQuery(context.Background(), &search_v1.SearchQueryRequest{
		IndexName: s.IndexName,
		Query: &search_v1.Query{
			Query: query,
		},
	}, grpc.PerRPCCredentials(s.getNoPermissionRpcCreds()))
	requireRpcSuccess(s.T(), client, err)

	_, err = queryResult.Recv()
	assertRpcStatus(s.T(), err, codes.PermissionDenied)
	assert.Contains(s.T(), err.Error(), "No permissions to query documents.")
}

func (s *testSearchServiceHelper) testCleanupSearch() {
	if s.IndexName != "" {
		_, err := s.IndexClient.DeleteIndex(context.Background(), &admin_search_v1.DeleteIndexRequest{
			Name: s.IndexName,
		})
		if err != nil {
			s.T().Logf("Failed to delete index: %s", err)
		}
	}
}

func (s *testSearchServiceHelper) testSetupSearch() {

	raw := s.loadTestData("testdata/beer_sample_brewery_five.json")

	var dataset []testBreweryDocumentJson
	err := json.Unmarshal(raw, &dataset)
	s.Require().NoErrorf(err, "Failed to unmarshal test data")

	for _, doc := range dataset {
		testDocId := fmt.Sprintf("search-%s", doc.Name)
		doc.Service = "search"

		b, err := json.Marshal(doc)
		s.Require().NoErrorf(err, "Failed to marshal search test document")

		s.createDocument(createDocumentOptions{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			DocId:          testDocId,
			Content:        b,
			ContentFlags:   TEST_CONTENT_FLAGS,
		})
	}

	s.IndexClient = admin_search_v1.NewSearchAdminServiceClient(s.gatewayConn)
	s.IndexName = "a" + uuid.NewString()[:6]
	s.Dataset = dataset

	sourceType := "couchbase"
	opts := &admin_search_v1.CreateIndexRequest{
		Name:       s.IndexName,
		SourceName: &s.bucketName,
		SourceType: &sourceType,
		Type:       "fulltext-index",
		Params:     make(map[string][]byte),
	}
	opts.Params["mapping"] = []byte(`{
		"default_analyzer": "standard",
		"default_datetime_parser": "dateTimeOptional",
		"default_field": "_all",
		"default_mapping": {
		 "dynamic": true,
		 "enabled": true,
		 "properties": {
		  "geo": {
		   "enabled": true,
		   "dynamic": false,
		   "fields": [
			{
			 "include_in_all": true,
			 "index": true,
			 "name": "geo",
			 "type": "geopoint"
			}
		   ]
		  }
		 }
		}
	}`)

	resp, err := s.IndexClient.CreateIndex(context.Background(), opts, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)
}

type testBreweryGeoJson struct {
	Accuracy string  `json:"accuracy,omitempty"`
	Lat      float32 `json:"lat,omitempty"`
	Lon      float32 `json:"lon,omitempty"`
}

type testBreweryDocumentJson struct {
	Capacity    int                `json:"capacity,omitempty"`
	City        string             `json:"city,omitempty"`
	Code        string             `json:"code,omitempty"`
	Country     string             `json:"country,omitempty"`
	Description string             `json:"description,omitempty"`
	DogFriendly bool               `json:"dogFriendly,omitempty"`
	Established string             `json:"established,omitempty"`
	Geo         testBreweryGeoJson `json:"geo,omitempty"`
	Name        string             `json:"name,omitempty"`
	Phone       string             `json:"phone,omitempty"`
	State       string             `json:"state,omitempty"`
	Type        string             `json:"type,omitempty"`
	Updated     string             `json:"updated,omitempty"`
	Website     string             `json:"website,omitempty"`

	Service string `json:"service,omitempty"`
}
