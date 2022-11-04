package hooks

import (
	"errors"
	"sync"

	"github.com/couchbase/stellar-nebula/genproto/internal_hooks_v1"
	"google.golang.org/grpc"
)

type HooksManager struct {
	lock          sync.Mutex
	hooksContexts map[string]*HooksContext
}

func NewHooksManager() *HooksManager {
	return &HooksManager{
		hooksContexts: make(map[string]*HooksContext),
	}
}

func (m *HooksManager) CreateHooksContext(hooksContextID string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, ok := m.hooksContexts[hooksContextID]
	if ok {
		return errors.New("existing hooks context already exists")
	}

	hooksContext := newHooksContext()
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

func (m *HooksManager) Server() internal_hooks_v1.HooksServer {
	return &grpcHooksServer{
		manager: m,
	}
}

func (m *HooksManager) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return makeGrpcUnaryInterceptor(m)
}
