package hooks

import (
	"errors"
	"sync"

	"github.com/couchbase/stellar-nebula/genproto/internal_hooks_v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type HooksManager struct {
	lock         sync.Mutex
	interceptors map[string]*Interceptor
}

func NewHooksManager() *HooksManager {
	return &HooksManager{
		interceptors: make(map[string]*Interceptor),
	}
}

func (m *HooksManager) CreateInterceptor() string {
	m.lock.Lock()
	defer m.lock.Unlock()

	interceptorID := uuid.NewString()

	interceptor := newInterceptor()
	m.interceptors[interceptorID] = interceptor

	return interceptorID
}

func (m *HooksManager) GetInterceptor(interceptorID string) *Interceptor {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.interceptors[interceptorID]
}

func (m *HooksManager) DestroyInterceptor(interceptorID string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, ok := m.interceptors[interceptorID]
	if !ok {
		return errors.New("invalid interceptor id")
	}

	delete(m.interceptors, interceptorID)
	return nil
}

func (m *HooksManager) Server() internal_hooks_v1.HooksServer {
	return &grpcHooksServer{
		manager: m,
	}
}

func (m *HooksManager) GrpcUnaryInterceptor() grpc.UnaryServerInterceptor {
	return makeGrpcInterceptor(m)
}
