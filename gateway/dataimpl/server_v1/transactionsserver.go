package server_v1

import (
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/goprotostellar/genproto/transactions_v1"
)

type TransactionsServer struct {
	transactions_v1.UnimplementedTransactionsServer
}

func NewTransactionsServer(cbClient *gocbcorex.AgentManager) *TransactionsServer {
	return &TransactionsServer{}
}
