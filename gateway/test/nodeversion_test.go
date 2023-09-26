package test

import (
	"errors"
	"strconv"
	"strings"
)

type NodeVersion struct {
	Major    int
	Minor    int
	Patch    int
	Build    int
	Edition  NodeEdition
	Modifier string
}

type NodeEdition int

const (
	CommunityNodeEdition  = NodeEdition(1)
	EnterpriseNodeEdition = NodeEdition(2)
)

func (v NodeVersion) Equal(ov NodeVersion) bool {
	if v.Major == ov.Major && v.Minor == ov.Minor &&
		v.Patch == ov.Patch && v.Build == ov.Build && v.Edition == ov.Edition && v.Modifier == ov.Modifier {
		return true
	}
	return false
}

func (v NodeVersion) Higher(ov NodeVersion) bool {
	if v.Major > ov.Major {
		return true
	} else if v.Major < ov.Major {
		return false
	}

	if v.Minor > ov.Minor {
		return true
	} else if v.Minor < ov.Minor {
		return false
	}

	if v.Patch > ov.Patch {
		return true
	} else if v.Patch < ov.Patch {
		return false
	}

	if v.Build > ov.Build {
		return true
	} else if v.Build < ov.Build {
		return false
	}

	if v.Edition > ov.Edition {
		return true
	}

	return false
}

func (v NodeVersion) Lower(ov NodeVersion) bool {
	return !v.Higher(ov) && !v.Equal(ov)
}

func (s *GatewayOpsTestSuite) nodeVersionFromString(version string) *NodeVersion {
	vSplit := strings.Split(version, ".")
	lenSplit := len(vSplit)
	if lenSplit == 0 {
		s.T().Fatalf("must provide at least a major version")
	}

	var err error
	nodeVersion := NodeVersion{}
	nodeVersion.Major, err = strconv.Atoi(vSplit[0])
	if err != nil {
		s.T().Fatalf("major version is not a valid integer")
	}
	if lenSplit == 1 {
		return &nodeVersion
	}

	nodeVersion.Minor, err = strconv.Atoi(vSplit[1])
	if err != nil {
		s.T().Fatalf("minor version is not a valid integer")
	}
	if lenSplit == 2 {
		return &nodeVersion
	}

	nodeBuild := strings.Split(vSplit[2], "-")
	nodeVersion.Patch, err = strconv.Atoi(nodeBuild[0])
	if err != nil {
		s.T().Fatalf("patch version is not a valid integer")
	}
	if len(nodeBuild) == 1 {
		return &nodeVersion
	}

	buildEdition := strings.Split(nodeBuild[1], "-")
	nodeVersion.Build, err = strconv.Atoi(buildEdition[0])
	if err != nil {
		edition, modifier, err := editionModifierFromString(buildEdition[0])
		s.Require().NoError(err)
		nodeVersion.Edition = edition
		nodeVersion.Modifier = modifier

		return &nodeVersion
	}
	if len(buildEdition) == 1 {
		return &nodeVersion
	}

	edition, modifier, err := editionModifierFromString(buildEdition[1])
	s.Require().NoError(err)
	nodeVersion.Edition = edition
	nodeVersion.Modifier = modifier

	return &nodeVersion
}

func editionModifierFromString(editionModifier string) (NodeEdition, string, error) {
	split := strings.Split(editionModifier, "-")
	editionStr := strings.ToLower(split[0])
	var edition NodeEdition
	var modifier string
	if editionStr == "enterprise" {
		edition = EnterpriseNodeEdition
	} else if editionStr == "community" {
		edition = CommunityNodeEdition
	} else if editionStr == "dp" {
		modifier = editionStr
	} else {
		return 0, "", errors.New("Unrecognised edition or modifier: " + editionStr)
	}
	if len(split) == 1 {
		return edition, modifier, nil
	}

	return edition, strings.ToLower(split[1]), nil
}
