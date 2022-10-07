package servers

import (
	"context"

	"github.com/couchbase/gocbcore/v10/memd"
	data_v1 "github.com/couchbase/stellar-nebula/genproto/data/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
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

	c.memdConn.WritePacket(&memd.Packet{
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
	})
	c.sendBasicReply(pak, memd.StatusSuccess, nil, resp.Content, nil)
}

func (c *KvServerClient) handleCmdSetReq(pak *memd.Packet) {
	// TODO(brett19): Implement the SET command.
	c.sendBasicReply(pak, memd.StatusSuccess, nil, nil, nil)

}

func (c *KvServerClient) handleCmdDeleteReq(pak *memd.Packet) {
	// TODO(brett19): Implement the DELETE command.
	c.sendBasicReply(pak, memd.StatusSuccess, nil, nil, nil)
}
