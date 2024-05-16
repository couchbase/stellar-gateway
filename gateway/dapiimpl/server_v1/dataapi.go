package server_v1

import (
	"strconv"

	"github.com/couchbase/stellar-gateway/dataapiv1"
	"go.uber.org/zap"
)

type DataApiServer struct {
	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
}

var _ dataapiv1.StrictServerInterface = &DataApiServer{}

func NewDataApiServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
) *DataApiServer {
	return &DataApiServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
	}
}

func (s *DataApiServer) parseKey(key string) ([]byte, *Status) {
	if len(key) > 250 || len(key) < 1 {
		return nil, s.errorHandler.NewInvalidKeyLengthStatus(key)
	}

	return []byte(key), nil
}

func (s *DataApiServer) parseCAS(etag *string) (uint64, *Status) {
	if etag != nil {
		casUint, err := strconv.ParseUint(*etag, 16, 64)
		if err != nil {
			return 0, s.errorHandler.NewInvalidEtagFormatStatus(*etag)
		}

		if casUint == 0 {
			return 0, s.errorHandler.NewZeroCasStatus()
		}

		return casUint, nil
	}

	return 0, nil
}
