/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package hooks

import (
	"errors"
	"sync"

	"github.com/couchbase/goprotostellar/genproto/internal_hooks_v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type HooksManager struct {
	lock          sync.Mutex
	hooksContexts map[string]*HooksContext
	logger        *zap.Logger
}

func NewHooksManager(logger *zap.Logger) *HooksManager {
	return &HooksManager{
		hooksContexts: make(map[string]*HooksContext),
		logger:        logger,
	}
}

func (m *HooksManager) CreateHooksContext(hooksContextID string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, ok := m.hooksContexts[hooksContextID]
	if ok {
		return errors.New("existing hooks context already exists")
	}

	hooksContext := newHooksContext(m.logger.Named("hook-context"))
	m.hooksContexts[hooksContextID] = hooksContext

	return nil
}

func (m *HooksManager) GetHooksContext(hooksContextID string) *HooksContext {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.hooksContexts[hooksContextID]
}

func (m *HooksManager) DestroyHooksContext(hooksContextID string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, ok := m.hooksContexts[hooksContextID]
	if !ok {
		return errors.New("invalid hooks context id")
	}

	delete(m.hooksContexts, hooksContextID)
	return nil
}

func (m *HooksManager) Server() internal_hooks_v1.HooksServiceServer {
	return &grpcHooksServer{
		manager: m,
		logger:  m.logger.Named("hooks-server"),
	}
}

func (m *HooksManager) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return makeGrpcUnaryInterceptor(m, m.logger)
}
