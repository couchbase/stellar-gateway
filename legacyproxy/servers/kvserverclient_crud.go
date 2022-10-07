package servers

import "github.com/couchbase/gocbcore/v10/memd"

func (c *KvServerClient) handleCmdGetReq(pak *memd.Packet) {
	// TODO(brett19): Implement the GET command.
	c.sendBasicReply(pak, memd.StatusSuccess, nil, nil, nil)
}

func (c *KvServerClient) handleCmdSetReq(pak *memd.Packet) {
	// TODO(brett19): Implement the SET command.
	c.sendBasicReply(pak, memd.StatusSuccess, nil, nil, nil)

}

func (c *KvServerClient) handleCmdDeleteReq(pak *memd.Packet) {
	// TODO(brett19): Implement the DELETE command.
	c.sendBasicReply(pak, memd.StatusSuccess, nil, nil, nil)
}
