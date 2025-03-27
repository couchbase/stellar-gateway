/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package servers

import "github.com/couchbase/gocbcore/v10/memd"

type ValidateFlags int

const (
	ValidateFlagAllowKey ValidateFlags = 1 << iota
	ValidateFlagAllowValue
	ValidateFlagAllowExtras
)

func (c *KvServerClient) validatePacket(pak *memd.Packet, flags ValidateFlags) bool {
	markInvalid := func(reason string) bool {
		c.sendInvalidArgs(pak, reason)
		return false
	}

	if flags&ValidateFlagAllowKey == 0 {
		if len(pak.Key) != 0 {
			return markInvalid("key should be empty")
		}
	}

	if flags&ValidateFlagAllowValue == 0 {
		if len(pak.Value) != 0 {
			return markInvalid("value should be empty")
		}
	}

	if flags&ValidateFlagAllowExtras == 0 {
		if len(pak.Extras) != 0 {
			return markInvalid("extras should be empty")
		}
	}

	return true
}
