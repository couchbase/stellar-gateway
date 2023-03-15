package server_v1

import (
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/goprotostellar/genproto/transactions_v1"
	"go.uber.org/zap"
)

type TransactionsServer struct {
	transactions_v1.UnimplementedTransactionsServiceServer

	logger   *zap.Logger
	cbClient *gocbcorex.AgentManager
}

func NewTransactionsServer(
	logger *zap.Logger,
	cbClient *gocbcorex.AgentManager,
) *TransactionsServer {
	return &TransactionsServer{
		logger:   logger,
		cbClient: cbClient,
	}
}
