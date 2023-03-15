package hooks

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/couchbase/goprotostellar/genproto/internal_hooks_v1"
	"github.com/couchbase/stellar-gateway/contrib/govalcmp"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// We encapsulate all the execution of actions into a runState to allow us to
// potentially maintain stateful debugging information about how the hooks are
// being executed
type runState struct {
	ID           string
	HooksContext *HooksContext
	Handler      grpc.UnaryHandler
	Hook         *internal_hooks_v1.Hook
	Logger       *zap.Logger
	ExecResult   interface{}
	ExecError    error
}

func newRunState(
	hooksContext *HooksContext,
	handler grpc.UnaryHandler,
	hook *internal_hooks_v1.Hook,
	logger *zap.Logger,
) *runState {
	return &runState{
		ID:           uuid.NewString(),
		HooksContext: hooksContext,
		Handler:      handler,
		Hook:         hook,
		Logger:       logger,
	}
}

func (s *runState) Run(ctx context.Context, req interface{}) (interface{}, error) {
	// actions run synchronously in respect to all hooks for this HooksContext, so we
	// need to acquire the run lock to be able to run...
	err := s.HooksContext.acquireRunLock(ctx)
	if err != nil {
		return nil, err
	}

	// run the actions
	resp, err := s.runActions(ctx, req, s.Hook.Actions)

	// release the run lock before finishing up
	s.HooksContext.releaseRunLock()

	// if the actions directly produced a result or error, we can return that
	if resp != nil || err != nil {
		return resp, err
	}

	// if the actions explicitly executed the underlying function, return its value
	if s.ExecResult != nil || s.ExecError != nil {
		return s.ExecResult, s.ExecError
	}

	// otherwise we need to implicitly execute the underlying function and return that
	return s.Handler(ctx, req)
}

func (s *runState) compare(
	left interface{},
	op internal_hooks_v1.ComparisonOperator,
	right interface{},
) (bool, error) {
	delta, err := govalcmp.Compare(left, right)
	if err != nil {
		return false, err
	}

	switch op {
	case internal_hooks_v1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL:
		return delta == 0, nil
	case internal_hooks_v1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN:
		return delta > 0, nil
	case internal_hooks_v1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL:
		return delta >= 0, nil
	case internal_hooks_v1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN:
		return delta < 0, nil
	case internal_hooks_v1.ComparisonOperator_COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL:
		return delta <= 0, nil
	}

	return false, errors.New("invalid comparison operator")
}

func (s *runState) resolveValueRef(
	ctx context.Context,
	req interface{},
	ref *internal_hooks_v1.ValueRef,
) (interface{}, error) {
	switch ref := ref.Value.(type) {
	case *internal_hooks_v1.ValueRef_CounterValue:
		return s.resolveValueRef_CounterValue(ctx, req, ref)
	case *internal_hooks_v1.ValueRef_RequestField:
		return s.resolveValueRef_RequestField(ctx, req, ref)
	case *internal_hooks_v1.ValueRef_JsonValue:
		return s.resolveValueRef_JsonValue(ctx, req, ref)
	}

	return nil, errors.New("invalid value ref")
}

func (s *runState) resolveValueRef_CounterValue(
	ctx context.Context,
	req interface{},
	ref *internal_hooks_v1.ValueRef_CounterValue,
) (interface{}, error) {
	counter := s.HooksContext.GetCounter(ref.CounterValue)
	return counter.Get(), nil
}

func (s *runState) resolveValueRef_RequestField(
	ctx context.Context,
	req interface{},
	ref *internal_hooks_v1.ValueRef_RequestField,
) (interface{}, error) {
	return nil, errors.New("unimplemented request field query")
}

func (s *runState) resolveValueRef_JsonValue(
	ctx context.Context,
	req interface{},
	ref *internal_hooks_v1.ValueRef_JsonValue,
) (interface{}, error) {
	var val interface{}
	err := json.Unmarshal(ref.JsonValue, &val)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (s *runState) checkConditions(
	ctx context.Context,
	req interface{},
	conds []*internal_hooks_v1.HookCondition,
) (bool, error) {
	for _, cond := range conds {
		ok, err := s.checkCondition(ctx, req, cond)
		if err != nil {
			return false, err
		}

		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func (s *runState) checkCondition(
	ctx context.Context,
	req interface{},
	cond *internal_hooks_v1.HookCondition,
) (bool, error) {
	leftVal, err := s.resolveValueRef(ctx, req, cond.Left)
	if err != nil {
		return false, err
	}

	rightVal, err := s.resolveValueRef(ctx, req, cond.Right)
	if err != nil {
		return false, err
	}

	ok, err := s.compare(leftVal, cond.Op, rightVal)
	if err != nil {
		return false, err
	}

	return ok, nil
}

// runActions runs a set of actions, failing on the first error that occurs, but allowing
// multiple things to create response objects.
func (s *runState) runActions(
	ctx context.Context,
	req interface{},
	actions []*internal_hooks_v1.HookAction,
) (interface{}, error) {
	var respOut interface{}

	for _, action := range actions {
		resp, err := s.runAction(ctx, req, action)
		if err != nil {
			return nil, err
		}

		if resp != nil {
			respOut = resp
		}
	}

	return respOut, nil
}

func (s *runState) runAction(
	ctx context.Context,
	req interface{},
	actions *internal_hooks_v1.HookAction,
) (interface{}, error) {
	switch action := actions.Action.(type) {
	case *internal_hooks_v1.HookAction_If_:
		return s.runAction_If(ctx, req, action.If)
	case *internal_hooks_v1.HookAction_Counter_:
		return s.runAction_Counter(ctx, req, action.Counter)
	case *internal_hooks_v1.HookAction_WaitOnBarrier_:
		return s.runAction_WaitOnBarrier(ctx, req, action.WaitOnBarrier)
	case *internal_hooks_v1.HookAction_SignalBarrier_:
		return s.runAction_SignalBarrier(ctx, req, action.SignalBarrier)
	case *internal_hooks_v1.HookAction_ReturnResponse_:
		return s.runAction_ReturnResponse(ctx, req, action.ReturnResponse)
	case *internal_hooks_v1.HookAction_ReturnError_:
		return s.runAction_ReturnError(ctx, req, action.ReturnError)
	case *internal_hooks_v1.HookAction_Execute_:
		return s.runAction_Execute(ctx, req, action.Execute)

	}

	return nil, errors.New("invalid hook action type")
}

func (s *runState) runAction_If(
	ctx context.Context,
	req interface{},
	action *internal_hooks_v1.HookAction_If,
) (interface{}, error) {
	ok, err := s.checkConditions(ctx, req, action.Cond)
	if err != nil {
		return nil, err
	}

	if ok {
		return s.runActions(ctx, req, action.Match)
	} else {
		return s.runActions(ctx, req, action.NoMatch)
	}
}

func (s *runState) runAction_Counter(
	ctx context.Context,
	req interface{},
	action *internal_hooks_v1.HookAction_Counter,
) (interface{}, error) {
	s.Logger.Info("hook incrementing counter", zap.Any("action",action))

	counter := s.HooksContext.getCounterLocked(action.CounterId)
	counter.Update(action.Delta)

	s.Logger.Info("hook incremented counter", zap.Any("action", action))

	return nil, nil
}

func (s *runState) runAction_WaitOnBarrier(
	ctx context.Context,
	req interface{},
	action *internal_hooks_v1.HookAction_WaitOnBarrier,
) (interface{}, error) {
	s.Logger.Info("hook waiting for barrier", zap.Any("action", action))

	barrier := s.HooksContext.getBarrierLocked(action.BarrierId)

	// we need to release the HooksContext runlock while we wait to allow
	// other calls to run while we are blocked waiting for the barrier.
	s.HooksContext.releaseRunLock()

	barrier.Wait(ctx, s.ID, nil)

	err := s.HooksContext.acquireRunLock(ctx)
	if err != nil {
		return nil, err
	}

	s.Logger.Info("hook waited for barrier", zap.Any("action", action))

	return nil, nil
}

func (s *runState) runAction_SignalBarrier(
	ctx context.Context,
	req interface{},
	action *internal_hooks_v1.HookAction_SignalBarrier,
) (interface{}, error) {
	s.Logger.Info("hook signalling barrier", zap.Any("action", action))

	barrier := s.HooksContext.GetBarrier(action.BarrierId)
	if action.SignalAll {
		barrier.SignalAll(nil)
	} else {
		barrier.TrySignalAny(nil)
	}

	s.Logger.Info("hook signaled for barrier", zap.Any("action", action))

	return nil, nil
}

func (s *runState) runAction_ReturnResponse(
	ctx context.Context,
	req interface{},
	action *internal_hooks_v1.HookAction_ReturnResponse,
) (interface{}, error) {
	return action.Value, nil
}

func (s *runState) runAction_ReturnError(
	ctx context.Context,
	req interface{},
	action *internal_hooks_v1.HookAction_ReturnError,
) (interface{}, error) {
	st := status.New(codes.Code(action.Code), action.Message)
	for _, detail := range action.Details {
		st, _ = st.WithDetails(detail)
	}

	return nil, st.Err()
}

func (s *runState) runAction_Execute(
	ctx context.Context,
	req interface{},
	action *internal_hooks_v1.HookAction_Execute,
) (interface{}, error) {
	s.ExecResult, s.ExecError = s.Handler(ctx, req)

	return nil, nil
}
