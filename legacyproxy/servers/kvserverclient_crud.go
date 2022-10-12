package servers

import (
	"context"
	"encoding/binary"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	data_v1 "github.com/couchbase/stellar-nebula/genproto/data/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func (c *KvServerClient) getOpContext(pak *memd.Packet) (context.Context, string, string, string) {
	// TODO(brett19): Implement collection resolution here...
	ctx := context.Background()

	// assign authentication data
	// TODO(brett19): Need to send the username/password here
	md := metadata.New(map[string]string{
		"authorization": "Bearer username:password",
	})

	ctx = metadata.NewIncomingContext(ctx, md)

	return ctx, c.selectedBucket, "", ""
}

func (c *KvServerClient) durabilityLevelToPs(dl memd.DurabilityLevel) (data_v1.DurabilityLevel, memd.StatusCode) {
	switch dl {
	case memd.DurabilityLevelMajority:
		return data_v1.DurabilityLevel_MAJORITY, memd.StatusSuccess
	case memd.DurabilityLevelMajorityAndPersistOnMaster:
		return data_v1.DurabilityLevel_MAJORITY_AND_PERSIST_TO_ACTIVE, memd.StatusSuccess
	case memd.DurabilityLevelPersistToMajority:
		return data_v1.DurabilityLevel_PERSIST_TO_MAJORITY, memd.StatusSuccess
	}

	return 0, memd.StatusDurabilityInvalidLevel
}

func (c *KvServerClient) flagsToPsContentType(flags uint32) data_v1.DocumentContentType {
	switch gocbcore.DataType(flags) {
	case gocbcore.BinaryType:
		return data_v1.DocumentContentType_BINARY
	case gocbcore.JSONType:
		return data_v1.DocumentContentType_JSON
	default:
		return data_v1.DocumentContentType_UNKNOWN
	}
}

func (c *KvServerClient) sendGrpcError(pak *memd.Packet, err error) {
	c.logger.Debug("encountered grpc error", zap.Error(err))
	c.sendBasicReply(pak, memd.StatusInternalError, nil, nil, nil)
}

func (c *KvServerClient) handleCmdGetReq(pak *memd.Packet) {
	ctx, bucketName, scopeName, collectionName := c.getOpContext(pak)

	resp, err := c.dataServer.Get(ctx, &data_v1.GetRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		Key:            string(pak.Key),
	})
	if err != nil {
		c.sendGrpcError(pak, err)
		return
	}

	if err := c.memdConn.WritePacket(&memd.Packet{
		Magic:        memd.CmdMagicRes,
		Command:      pak.Command,
		Datatype:     0,
		Status:       memd.StatusSuccess,
		Vbucket:      pak.Vbucket,
		Opaque:       pak.Opaque,
		Cas:          resp.Cas,
		CollectionID: 0,
		Key:          nil,
		Extras:       nil,
		Value:        resp.Content,
	}); err != nil {
		c.sendBasicReply(pak, memd.StatusInternalError, nil, nil, nil)
		return
	}
	c.sendBasicReply(pak, memd.StatusSuccess, nil, resp.Content, nil)
}

func (c *KvServerClient) handleCmdSetReq(pak *memd.Packet) {
	ctx, bucketName, scopeName, collectionName := c.getOpContext(pak)

	req := &data_v1.UpsertRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		Key:            string(pak.Key),
		Content:        pak.Value,
	}
	if pak.DurabilityLevelFrame != nil {
		dl, s := c.durabilityLevelToPs(pak.DurabilityLevelFrame.DurabilityLevel)
		if dl == 0 {
			c.sendBasicReply(pak, s, nil, nil, nil)
			return
		}
		req.DurabilitySpec = &data_v1.UpsertRequest_DurabilityLevel{
			DurabilityLevel: dl,
		}
	}
	if len(pak.Extras) >= 4 {
		flags := binary.BigEndian.Uint32(pak.Extras[0:])
		req.ContentType = c.flagsToPsContentType(flags)

		if len(pak.Extras) >= 8 {
			expiry := binary.BigEndian.Uint32(pak.Extras[4:])
			req.Expiry = timestamppb.New(time.Now().Add(time.Duration(expiry) * time.Second))
		}
	}

	resp, err := c.dataServer.Upsert(ctx, req)
	if err != nil {
		c.sendGrpcError(pak, err)
		return
	}

	if err := c.memdConn.WritePacket(&memd.Packet{
		Magic:        memd.CmdMagicRes,
		Command:      pak.Command,
		Datatype:     0,
		Status:       memd.StatusSuccess,
		Vbucket:      pak.Vbucket,
		Opaque:       pak.Opaque,
		Cas:          resp.Cas,
		CollectionID: 0,
		Key:          nil,
		Extras:       nil,
		Value:        nil,
	}); err != nil {
		c.sendBasicReply(pak, memd.StatusInternalError, nil, nil, nil)
		return
	}

	c.sendBasicReply(pak, memd.StatusSuccess, nil, nil, nil)

}

func (c *KvServerClient) handleCmdDeleteReq(pak *memd.Packet) {
	ctx, bucketName, scopeName, collectionName := c.getOpContext(pak)

	req := &data_v1.RemoveRequest{
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collectionName,
		Key:            string(pak.Key),
	}
	if pak.DurabilityLevelFrame != nil {
		dl, s := c.durabilityLevelToPs(pak.DurabilityLevelFrame.DurabilityLevel)
		if dl == 0 {
			c.sendBasicReply(pak, s, nil, nil, nil)
			return
		}
		req.DurabilitySpec = &data_v1.RemoveRequest_DurabilityLevel{
			DurabilityLevel: dl,
		}
	}

	resp, err := c.dataServer.Remove(ctx, req)
	if err != nil {
		c.sendGrpcError(pak, err)
		return
	}

	if err := c.memdConn.WritePacket(&memd.Packet{
		Magic:        memd.CmdMagicRes,
		Command:      pak.Command,
		Datatype:     0,
		Status:       memd.StatusSuccess,
		Vbucket:      pak.Vbucket,
		Opaque:       pak.Opaque,
		Cas:          resp.Cas,
		CollectionID: 0,
		Key:          nil,
		Extras:       nil,
		Value:        nil,
	}); err != nil {
		c.sendBasicReply(pak, memd.StatusInternalError, nil, nil, nil)
		return
	}

	c.sendBasicReply(pak, memd.StatusSuccess, nil, nil, nil)
}
