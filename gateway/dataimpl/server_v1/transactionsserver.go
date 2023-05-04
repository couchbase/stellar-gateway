package server_v1

import (
	"github.com/couchbase/goprotostellar/genproto/transactions_v1"
	"go.uber.org/zap"
)

type TransactionsServer struct {
	transactions_v1.UnimplementedTransactionsServiceServer

	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
}

func NewTransactionsServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
) *TransactionsServer {
	return &TransactionsServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
	}
}
