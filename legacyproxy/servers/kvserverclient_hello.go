package servers

import (
	"encoding/binary"

	"github.com/couchbase/gocbcore/v10/memd"
	"go.uber.org/zap"
)

func (c *KvServerClient) isSupportedFeature(feat memd.HelloFeature) bool {
	// TODO(brett19): Implement checking of supported memcached features
	return true
}

func (c *KvServerClient) handleCmdHelloReq(pak *memd.Packet) {
	if !c.validatePacket(pak, ValidateFlagAllowKey|ValidateFlagAllowValue) {
		return
	}

	if len(pak.Value)%2 != 0 {
		c.sendInvalidArgs(pak, "value length not divisible by 2")
		return
	}

	// parse all the features
	var features []memd.HelloFeature
	for i := 0; i < len(pak.Value); i += 2 {
		feature := binary.BigEndian.Uint16(pak.Value[i:])
		features = append(features, memd.HelloFeature(feature))
	}

	// record the helloed client name
	c.helloName = string(pak.Key)

	// enable any features we support that were requested
	var enabledFeatures []memd.HelloFeature
	for _, feat := range features {
		if c.isSupportedFeature(feat) {
			// enable the features on our protocol parser
			c.memdConn.EnableFeature(feat)

			// mark the feature as enabled for the reply
			enabledFeatures = append(enabledFeatures, feat)
		}
	}

	// generate the reply payload
	enabledFeatureBytes := make([]byte, len(enabledFeatures)*2)
	for featIdx, feat := range enabledFeatures {
		binary.BigEndian.PutUint16(enabledFeatureBytes[featIdx*2:], uint16(feat))
	}

	// reply saying we support all the features they asked for.
	c.sendSuccessReply(pak, nil, enabledFeatureBytes, nil)

	// TODO(brett19): HelloFeature should have a String() method.
	c.logger.Debug("client hello completed",
		zap.Any("features", features),
	)
}
