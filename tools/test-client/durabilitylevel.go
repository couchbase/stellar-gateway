package gocbps

import (
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
)

// DurabilityLevel specifies the level of synchronous replication to use.
type DurabilityLevel uint8

const (
	// DurabilityLevelUnknown specifies that the durability level is not set and will default to the default durability level.
	DurabilityLevelUnknown DurabilityLevel = iota

	// DurabilityLevelNone specifies that no durability level should be applied.
	DurabilityLevelNone

	// DurabilityLevelMajority specifies that a mutation must be replicated (held in memory) to a majority of nodes.
	DurabilityLevelMajority

	// DurabilityLevelMajorityAndPersistOnMaster specifies that a mutation must be replicated (held in memory) to a
	// majority of nodes and also persisted (written to disk) on the active node.
	DurabilityLevelMajorityAndPersistOnMaster

	// DurabilityLevelPersistToMajority specifies that a mutation must be persisted (written to disk) to a majority
	// of nodes.
	DurabilityLevelPersistToMajority
)

func (dl DurabilityLevel) toProto() *kv_v1.DurabilityLevel {
	var durability kv_v1.DurabilityLevel
	switch dl {
	case DurabilityLevelMajority:
		durability = kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY
	case DurabilityLevelMajorityAndPersistOnMaster:
		durability = kv_v1.DurabilityLevel_DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE
	case DurabilityLevelPersistToMajority:
		durability = kv_v1.DurabilityLevel_DURABILITY_LEVEL_PERSIST_TO_MAJORITY
	default:
		return nil
	}

	return &durability
}
