package hooks

import (
	"context"
	"log"

	"github.com/couchbase/stellar-nebula/genproto/internal_hooks_v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcHooksServer struct {
	internal_hooks_v1.UnimplementedHooksServer

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
	s.manager.DestroyHooksContext(req.Id)

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
	stream internal_hooks_v1.Hooks_WatchBarrierServer,
) error {
	hooksContext := s.manager.GetHooksContext(req.HooksContextId)
	if hooksContext == nil {
		return status.Errorf(codes.NotFound, "invalid hooks context id")
	}

	log.Printf("starting counter watcher")

	barrier := hooksContext.GetBarrier(req.BarrierId)
	watchCtx, watchCancel := context.WithCancel(stream.Context())
	watchCh := barrier.Watch(watchCtx)

	log.Printf("started barrier watcher")

	// we need to guarentee that we don't block the watch channel, so we move the data
	// into a buffered channel with 1024 slots, if the buffered channel becomes too filled,
	// we close the watcher connection because its being too slow.
	bufWatchCh := make(chan *BarrierWaiter, 1024)
	go func() {
		for {
			newWaiter := <-watchCh

			select {
			case bufWatchCh <- newWaiter:
			default:
				// client is too slow
				watchCancel()
				close(bufWatchCh)
			}
		}
	}()

	for {
		newWaiter := <-bufWatchCh

		log.Printf("sending barrier watch value: %v", newWaiter)
		err := stream.Send(&internal_hooks_v1.WatchBarrierResponse{
			WaitId: newWaiter.ID,
		})
		if err != nil {
			log.Printf("failed to write barrier watch value: %s", err)
			return err
		}
		log.Printf("sent barrier watch value: %v", newWaiter)
	}
}

func (s *grpcHooksServer) SignalBarrier(
	ctx context.Context,
	req *internal_hooks_v1.SignalBarrierRequest,
) (*internal_hooks_v1.SignalBarrierResponse, error) {
	log.Printf("grpc signalling barrier: %+v", req)

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

	log.Printf("grpc signaled barrier: %+v", req)

	return &internal_hooks_v1.SignalBarrierResponse{}, nil
}
