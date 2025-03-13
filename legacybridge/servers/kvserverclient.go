/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package servers

import (
	"bufio"
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/stellar-gateway/contrib/scramserver"
	"github.com/couchbase/stellar-gateway/legacybridge/topology"
	"go.uber.org/zap"
)

type KvServerClientOptions struct {
	Logger           *zap.Logger
	ParentServer     *KvServer
	TopologyProvider topology.Provider
	KvClient         kv_v1.KvServiceClient
	Conn             net.Conn
}

type KvServerClient struct {
	logger           *zap.Logger
	parentServer     *KvServer
	topologyProvider topology.Provider
	kvClient         kv_v1.KvServiceClient
	conn             net.Conn

	memdConn       *memd.Conn
	scramServer    *scramserver.ScramServer
	helloName      string
	authUser       string
	selectedBucket string
}

// TODO(brett19): We should move wrappedReaderWriter somewhere...
type wrappedReadWriter struct {
	*bufio.Reader
	io.Writer
}

func makeBufferedMemdConn(s io.ReadWriter) *memd.Conn {
	return memd.NewConn(wrappedReadWriter{
		Reader: bufio.NewReader(s),
		Writer: s,
	})
}

func NewKvServerClient(opts *KvServerClientOptions) (*KvServerClient, error) {
	client := &KvServerClient{
		logger:           opts.Logger,
		parentServer:     opts.ParentServer,
		kvClient:         opts.KvClient,
		topologyProvider: opts.TopologyProvider,
		conn:             opts.Conn,
	}

	err := client.init()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (c *KvServerClient) init() error {
	c.memdConn = makeBufferedMemdConn(c.conn)

	go c.procThread()
	return nil
}

// TODO(brett19): This isClosedErr function should not exist here (or maybe anywhere)...
func isClosedErr(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, net.ErrClosed)
}

func (c *KvServerClient) procThread() {
	// run the client to server proxying...
	for {
		pak, _, err := c.memdConn.ReadPacket()
		if err != nil {
			// dont log when this is a 'generally expected' closing of the connection
			if !isClosedErr(err) {
				c.logger.Warn("Unexpected srv read error", zap.Error(err))
			}
			break
		}

		c.logger.Debug("received packet",
			zap.Stringer("packetdata", memdPacketStringer{pak}),
		)
		c.handlePacket(pak)
	}

	err := c.conn.Close()
	if err != nil {
		if !isClosedErr(err) {
			c.logger.Warn("failed to disconnect server conn after client conn close", zap.Error(err))
		}
	}

	c.handleDisconnect()
}

func (c *KvServerClient) sendBasicReply(
	reqPak *memd.Packet,
	status memd.StatusCode,
	key, value, extras []byte,
) {
	respPak := &memd.Packet{
		Magic:        memd.CmdMagicRes,
		Command:      reqPak.Command,
		Datatype:     0,
		Status:       status,
		Vbucket:      0,
		Opaque:       reqPak.Opaque,
		Cas:          0,
		CollectionID: 0,
		Key:          key,
		Extras:       extras,
		Value:        value,
	}

	err := c.memdConn.WritePacket(respPak)
	if err != nil {
		c.logger.Debug("failed to write packet", zap.Error(err), zap.Any("packet", respPak))
	}
}

func (c *KvServerClient) sendSuccessReply(
	reqPak *memd.Packet,
	key, value, extras []byte,
) {
	c.sendBasicReply(reqPak, memd.StatusSuccess, key, value, extras)
}

func (c *KvServerClient) sendInvalidArgs(
	reqPak *memd.Packet,
	reason string,
) {
	c.logger.Debug(
		"invalid arguments sent for packet",
		zap.String("reason", reason),
		zap.Stringer("packet", memdPacketStringer{reqPak}))

	c.sendBasicReply(reqPak, memd.StatusInvalidArgs, nil, nil, nil)
}

func (c *KvServerClient) sendInternalError(
	reqPak *memd.Packet,
	err error,
) {
	c.logger.Debug(
		"internal error",
		zap.Error(err),
		zap.Stringer("packet", memdPacketStringer{reqPak}))

	c.sendBasicReply(reqPak, memd.StatusInternalError, nil, nil, nil)
}

func (c *KvServerClient) sendUnknownCommand(
	reqPak *memd.Packet,
) {
	c.logger.Debug(
		"unknown command",
		zap.Stringer("packet", memdPacketStringer{reqPak}))

	c.sendBasicReply(reqPak, memd.StatusUnknownCommand, nil, nil, nil)
}

func (c *KvServerClient) handlePacket(pak *memd.Packet) {
	if pak.Magic == memd.CmdMagicReq {
		switch pak.Command {
		// bootstrap commands
		case memd.CmdHello:
			c.handleCmdHelloReq(pak)
		case memd.CmdGetErrorMap:
			c.handleCmdGetErrorMapReq(pak)
		case memd.CmdGetClusterConfig:
			c.handleCmdGetClusterConfigReq(pak)
		case memd.CmdSelectBucket:
			c.handleCmdSelectBucketReq(pak)

			// auth commands
		case memd.CmdSASLListMechs:
			c.handleCmdSASLListMechsReq(pak)
		case memd.CmdSASLAuth:
			c.handleCmdSASLAuthReq(pak)
		case memd.CmdSASLStep:
			c.handleCmdSASLStepReq(pak)

			// crud commands
		case memd.CmdGet:
			c.handleCmdGetReq(pak)
		case memd.CmdSet:
			c.handleCmdSetReq(pak)
		case memd.CmdDelete:
			c.handleCmdDeleteReq(pak)

		default:
			c.sendUnknownCommand(pak)
		}
	} else if pak.Magic == memd.CmdMagicRes {
		switch pak.Command {
		default:
			c.sendUnknownCommand(pak)
		}
	} else {
		c.sendUnknownCommand(pak)
	}
}

func (c *KvServerClient) handleDisconnect() {
	// notify our parent that we've disconnected
	c.parentServer.handleClientDisconnect(c)
}
