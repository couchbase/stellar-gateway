package server_v1

import (
	"context"
	"strconv"
	"time"

	"github.com/couchbase/stellar-gateway/dataapiv1"
	"go.uber.org/zap"
)

const defaultDapiKvTimeout = 120 * time.Second

type DataApiServer struct {
	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler

	kvTimeout time.Duration
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
		kvTimeout:    defaultDapiKvTimeout,
	}
}

func (s *DataApiServer) parseKey(key string) ([]byte, *Status) {
	if len(key) > 250 || len(key) < 1 {
		return nil, s.errorHandler.NewInvalidKeyLengthStatus(key)
	}

	return []byte(key), nil
}

func (s *DataApiServer) withKvTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.kvTimeout)
}

func (s *DataApiServer) ReconfigureKvTimeout(timeout time.Duration) {
	if timeout <= 0 {
		// Ignore non-positive values; keep existing timeout.
		s.logger.Warn("ignoring non-positive data api kv timeout", zap.Duration("timeout", timeout))
		return
	}

	if timeout < time.Second {
		s.logger.Warn("data api kv timeout too low; coercing to 1s", zap.Duration("requested_timeout", timeout))
		timeout = time.Second
	}

	if timeout > defaultDapiKvTimeout {
		s.logger.Warn("data api kv timeout too high; coercing to 120s", zap.Duration("requested_timeout", timeout))
		timeout = defaultDapiKvTimeout
	}

	s.logger.Info("reconfiguring data api kv timeout", zap.Duration("timeout", timeout))
	s.kvTimeout = timeout
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
