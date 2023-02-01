package servers

import (
	"strings"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/stellar-gateway/contrib/scramserver"
	"go.uber.org/zap"
)

func (c *KvServerClient) handleCmdSASLListMechsReq(pak *memd.Packet) {
	if !c.validatePacket(pak, 0) {
		return
	}

	// TODO(brett19): Implement actual auth mechanism configuration support.
	supportedMechs := []string{
		"PLAIN",
		"SCRAM-SHA1",
		"SCRAM-SHA256",
		"SCRAM-SHA512",
	}

	supportedBytes := []byte(strings.Join(supportedMechs, " "))

	c.sendSuccessReply(pak, nil, supportedBytes, nil)
}

func (c *KvServerClient) handleCmdSASLAuthReq_PLAIN(pak *memd.Packet) {
	// TODO(brett19): Add support for PLAIN authentication
	c.sendInvalidArgs(pak, "PLAIN not yet supported")
}

func (c *KvServerClient) handleCmdSASLAuthReq_SCRAM(pak *memd.Packet, authMech string) {
	if c.scramServer != nil {
		c.sendInvalidArgs(pak, "scram already started")
		return
	}

	scramServer := &scramserver.ScramServer{}
	respBytes, err := scramServer.Start(pak.Value, authMech)
	if err != nil {
		c.logger.Debug("failed to start scram authentication", zap.Error(err))
		c.sendBasicReply(pak, memd.StatusAuthError, nil, nil, nil)
		return
	}

	if respBytes == nil {
		// scram is already complete, but we don't support this...
		c.logger.Debug("invalid initial scram state")
		c.sendBasicReply(pak, memd.StatusAuthError, nil, nil, nil)
		return
	}

	// TODO(brett19): Implement actual authentication
	username := scramServer.Username()
	if username != "Administrator" {
		c.logger.Debug("kv client sent invalid username", zap.String("username", username))
		c.sendBasicReply(pak, memd.StatusAccessError, nil, nil, nil)
		return
	}
	err = scramServer.SetPassword("password")
	if err != nil {
		c.logger.Debug("failed to setup scram password", zap.Error(err))
		c.sendBasicReply(pak, memd.StatusAuthError, nil, nil, nil)
		return
	}

	c.scramServer = scramServer
	c.sendBasicReply(pak, memd.StatusAuthContinue, nil, respBytes, nil)
}

func (c *KvServerClient) handleCmdSASLAuthReq(pak *memd.Packet) {
	if !c.validatePacket(pak, ValidateFlagAllowKey|ValidateFlagAllowValue) {
		return
	}

	// TODO(brett19): The auth mechanism passed to ScramServer should not be prefixed.
	// It currently has the SCRAM- prefix which seems incorrect... They should match
	// the names of hash functions that are used throughout Go.

	authMech := string(pak.Key)
	switch authMech {
	case "PLAIN":
		c.handleCmdSASLAuthReq_PLAIN(pak)
	case "SCRAM-SHA1":
		c.handleCmdSASLAuthReq_SCRAM(pak, "SCRAM-SHA1")
	case "SCRAM-SHA256":
		c.handleCmdSASLAuthReq_SCRAM(pak, "SCRAM-SHA256")
	case "SCRAM-SHA512":
		c.handleCmdSASLAuthReq_SCRAM(pak, "SCRAM-SHA512")
	default:
		c.sendInvalidArgs(pak, "invalid auth mechanism")
	}
}

func (c *KvServerClient) handleCmdSASLStepReq_PLAIN(pak *memd.Packet) {
	c.sendInvalidArgs(pak, "cannot step PLAIN auth")
}

func (c *KvServerClient) handleCmdSASLStepReq_SCRAM(pak *memd.Packet, authMech string) {
	if c.scramServer == nil {
		c.sendInvalidArgs(pak, "scram not started started")
		return
	}

	scramServer := c.scramServer
	c.scramServer = nil

	respBytes, err := scramServer.Step(pak.Value)
	if err != nil {
		c.logger.Debug("failed to step scram authentication", zap.Error(err))
		c.sendBasicReply(pak, memd.StatusAuthError, nil, nil, nil)
		return
	}

	c.authUser = scramServer.Username()
	c.sendBasicReply(pak, memd.StatusSuccess, nil, respBytes, nil)
}

func (c *KvServerClient) handleCmdSASLStepReq(pak *memd.Packet) {
	if !c.validatePacket(pak, ValidateFlagAllowKey|ValidateFlagAllowValue) {
		return
	}

	authMech := string(pak.Key)
	switch authMech {
	case "PLAIN":
		c.handleCmdSASLStepReq_PLAIN(pak)
	case "SCRAM-SHA1":
		c.handleCmdSASLStepReq_SCRAM(pak, "SCRAM-SHA1")
	case "SCRAM-SHA256":
		c.handleCmdSASLStepReq_SCRAM(pak, "SCRAM-SHA256")
	case "SCRAM-SHA512":
		c.handleCmdSASLStepReq_SCRAM(pak, "SCRAM-SHA512")
	default:
		c.sendInvalidArgs(pak, "invalid auth mechanism")
	}
}
