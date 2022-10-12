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
