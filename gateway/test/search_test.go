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
	"google.golang.org/grpc"
)

func (s *GatewayOpsTestSuite) TestSearchBasic() {
	helper := &testSearchServiceHelper{
		GatewayOpsTestSuite: s,
	}

	if s.Run("SearchSetup", helper.testSetupSearch) {

		s.Run("Test", helper.testSearchBasic)

		s.Run("Cleanup", helper.testCleanupSearch)
	}
}

type testSearchServiceHelper struct {
	*GatewayOpsTestSuite
	IndexName   string
	Dataset     []testBreweryDocumentJson
	IndexClient admin_search_v1.SearchAdminServiceClient
}

func (s *testSearchServiceHelper) testSearchBasic() {
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
		var min float32 = 30
		var max float32 = 31
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
							Field: "geo.lat",
							Size:  5,
							NumericRanges: []*search_v1.NumericRange{
								{
									Name: "lat",
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
	}, 60*time.Second, 500*time.Millisecond)

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
			s.Equal(int64(1), f.NumericRangeFacet.Total)
			s.Equal("geo.lat", f.NumericRangeFacet.Field)
			s.Equal(1, len(f.NumericRangeFacet.NumericRanges))
			s.Equal(uint64(1), f.NumericRangeFacet.NumericRanges[0].Size)
			s.Equal(uint64(30), f.NumericRangeFacet.NumericRanges[0].Min)
			s.Equal(uint64(31), f.NumericRangeFacet.NumericRanges[0].Max)
			s.Equal("lat", f.NumericRangeFacet.NumericRanges[0].Name)
		}
	}
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
	resp, err := s.IndexClient.UpsertIndex(context.Background(), &admin_search_v1.UpsertIndexRequest{
		Name:       s.IndexName,
		SourceName: &s.bucketName,
		SourceType: &sourceType,
		Type:       "fulltext-index",
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)
}

type testBreweryGeoJson struct {
	Accuracy string  `json:"accuracy,omitempty"`
	Lat      float32 `json:"lat,omitempty"`
	Lon      float32 `json:"lon,omitempty"`
}

type testBreweryDocumentJson struct {
	City        string             `json:"city,omitempty"`
	Code        string             `json:"code,omitempty"`
	Country     string             `json:"country,omitempty"`
	Description string             `json:"description,omitempty"`
	Geo         testBreweryGeoJson `json:"geo,omitempty"`
	Name        string             `json:"name,omitempty"`
	Phone       string             `json:"phone,omitempty"`
	State       string             `json:"state,omitempty"`
	Type        string             `json:"type,omitempty"`
	Updated     string             `json:"updated,omitempty"`
	Website     string             `json:"website,omitempty"`

	Service string `json:"service,omitempty"`
}
