/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package test

const DefaultClusterVer = "7.2.1"

type TestFeatureCode string

var (
	TestFeatureKV                          = TestFeatureCode("kv")
	TestFeatureSearch                      = TestFeatureCode("search")
	TestFeatureQuery                       = TestFeatureCode("query")
	TestFeatureQueryManagement             = TestFeatureCode("querymgmt")
	TestFeatureSearchManagement            = TestFeatureCode("searchmgmt")
	TestFeatureSearchManagementCollections = TestFeatureCode("searchmgmtcollections")
	TestFeatureBucketManagement            = TestFeatureCode("bucketmgmt")
	TestFeatureCollectionNoExpriy          = TestFeatureCode("collectionnoexpiry")
)

type TestFeature struct {
	Code    TestFeatureCode
	Enabled bool
}

var (
	SrvVer721 = NodeVersion{7, 2, 1, 0, 0, ""}
	SrvVer750 = NodeVersion{7, 5, 0, 0, 0, ""}
	SrvVer760 = NodeVersion{7, 6, 0, 0, 0, ""}
)

func (s *GatewayOpsTestSuite) SupportsFeature(code TestFeatureCode) bool {
	featureFlagValue := 0
	for _, featureFlag := range s.features {
		if featureFlag.Code == code || featureFlag.Code == "*" {
			if featureFlag.Enabled {
				featureFlagValue = +1
			} else {
				featureFlagValue = -1
			}
		}
	}
	if featureFlagValue == +1 {
		return true
	} else if featureFlagValue == -1 {
		return false
	}

	switch code {
	case TestFeatureKV:
		return true
	case TestFeatureSearch:
		return true
	case TestFeatureQuery:
		return true
	case TestFeatureBucketManagement:
		return true
	case TestFeatureQueryManagement:
		return true
	case TestFeatureSearchManagement:
		return true
	case TestFeatureSearchManagementCollections:
		return !s.clusterVersion.Lower(SrvVer750)
	case TestFeatureCollectionNoExpriy:
		return !s.clusterVersion.Lower(SrvVer760)
	}

	panic("found unsupported feature code")
}
