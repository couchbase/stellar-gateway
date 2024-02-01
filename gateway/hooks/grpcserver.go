package hooks

import (
	"context"

	"github.com/couchbase/goprotostellar/genproto/internal_hooks_v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcHooksServer struct {
	internal_hooks_v1.UnimplementedHooksServiceServer
	logger  *zap.Logger
	manager *HooksManager
}

func (s *grpcHooksServer) CreateHooksContext(
	ctx context.Context,
	req *internal_hooks_v1.CreateHooksContextRequest,
) (*internal_hooks_v1.CreateHooksContextResponse, error) {
	err := s.manager.CreateHooksContext(req.Id)
	if err != nil {
		return nil, err
	}

	return &internal_hooks_v1.CreateHooksContextResponse{}, nil
}

func (s *grpcHooksServer) DestroyHooksContext(
	ctx context.Context,
	req *internal_hooks_v1.DestroyHooksContextRequest,
) (*internal_hooks_v1.DestroyHooksContextResponse, error) {
	err := s.manager.DestroyHooksContext(req.Id)
	if err != nil {
		return nil, err
	}

	return &internal_hooks_v1.DestroyHooksContextResponse{}, nil
}

func (s *grpcHooksServer) AddHooks(
	ctx context.Context,
	req *internal_hooks_v1.AddHooksRequest,
) (*internal_hooks_v1.AddHooksResponse, error) {
	hooksContext := s.manager.GetHooksContext(req.HooksContextId)
	if hooksContext == nil {
		return nil, status.Errorf(codes.NotFound, "invalid hooks context id")
	}

	// register all the hooks
	for _, hook := range req.Hooks {
		hooksContext.AddHook(hook)
	}

	return &internal_hooks_v1.AddHooksResponse{}, nil
}

func (s *grpcHooksServer) WatchBarrier(
	req *internal_hooks_v1.WatchBarrierRequest,
	stream internal_hooks_v1.HooksService_WatchBarrierServer,
) error {
	hooksContext := s.manager.GetHooksContext(req.HooksContextId)
	if hooksContext == nil {
		return status.Errorf(codes.NotFound, "invalid hooks context id")
	}

	s.logger.Info("starting barrier watcher")

	barrier := hooksContext.GetBarrier(req.BarrierId)
	watchCtx, watchCancel := context.WithCancel(stream.Context())
	watchCh := barrier.Watch(watchCtx)

	s.logger.Info("started barrier watcher")

	// we need to guarentee that we don't block the watch channel, so we move the data
	// into a buffered channel with 1024 slots, if the buffered channel becomes too filled,
	// we close the watcher connection because its being too slow.
	bufWatchCh := make(chan *BarrierWaiter, 1024)
	go func() {
		for {
			newWaiter, ok := <-watchCh
			if !ok {
				watchCancel()
				close(bufWatchCh)
				return
			}

			select {
			case bufWatchCh <- newWaiter:
			default:
				// client is too slow
				watchCancel()
				close(bufWatchCh)
				return
			}
		}
	}()

	for {
		newWaiter, ok := <-bufWatchCh
		if !ok {
			break
		}

		s.logger.Info("sending barrier watch value", zap.Any("barrier-watch-value", newWaiter))
		err := stream.Send(&internal_hooks_v1.WatchBarrierResponse{
			WaitId: newWaiter.ID,
		})
		if err != nil {
			s.logger.Error("failed to write barrier watch value", zap.Error(err))
			return err
		}
		s.logger.Info("sent barrier watch value", zap.Any("barrier-watch-value", newWaiter))
	}

	return nil
}

func (s *grpcHooksServer) SignalBarrier(
	ctx context.Context,
	req *internal_hooks_v1.SignalBarrierRequest,
) (*internal_hooks_v1.SignalBarrierResponse, error) {
	s.logger.Info("grpc signalling barrier", zap.Any("signal-barrier", req))

	hooksContext := s.manager.GetHooksContext(req.HooksContextId)
	if hooksContext == nil {
		return nil, status.Errorf(codes.NotFound, "invalid hooks context id")
	}

	barrier := hooksContext.GetBarrier(req.BarrierId)

	if req.WaitId == nil {
		barrier.TrySignalAny(nil)
	} else {
		barrier.TrySignal(*req.WaitId, nil)
	}

	s.logger.Info("grpc signaled barrier", zap.Any("signal-barrier", req))

	return &internal_hooks_v1.SignalBarrierResponse{}, nil
}
