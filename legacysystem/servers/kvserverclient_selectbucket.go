package servers

import (
	"github.com/couchbase/gocbcore/v10/memd"
)

func (c *KvServerClient) handleCmdSelectBucketReq(pak *memd.Packet) {
	if !c.validatePacket(pak, ValidateFlagAllowKey) {
		return
	}

	bucketName := string(pak.Key)

	// TODO(brett19): Implement bucket validation

	c.selectedBucket = bucketName

	c.sendSuccessReply(pak, nil, nil, nil)
}
