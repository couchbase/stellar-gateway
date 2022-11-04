package hooks

import (
	"context"
	"log"

	"github.com/couchbase/stellar-nebula/genproto/internal_hooks_v1"
	"github.com/couchbase/stellar-nebula/utils/latestonlychannel"
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

func (s *grpcHooksServer) UpdateCounter(
	ctx context.Context,
	req *internal_hooks_v1.UpdateCounterRequest,
) (*internal_hooks_v1.UpdateCounterResponse, error) {
	hooksContext := s.manager.GetHooksContext(req.HooksContextId)
	if hooksContext == nil {
		return nil, status.Errorf(codes.NotFound, "invalid hooks context id")
	}

	counter := hooksContext.GetCounter(req.CounterId)
	newValue := counter.Update(req.Delta)

	return &internal_hooks_v1.UpdateCounterResponse{
		Value: newValue,
	}, nil
}

func (s *grpcHooksServer) WatchCounter(
	req *internal_hooks_v1.WatchCounterRequest,
	stream internal_hooks_v1.Hooks_WatchCounterServer,
) error {
	hooksContext := s.manager.GetHooksContext(req.HooksContextId)
	if hooksContext == nil {
		return status.Errorf(codes.NotFound, "invalid hooks context id")
	}

	log.Printf("starting counter watcher")

	counter := hooksContext.GetCounter(req.CounterId)
	watchCh := counter.Watch(stream.Context())

	log.Printf("started counter watcher")

	// we need to guarentee that we don't block the channel, so we wrap it in
	// a watcher that only returns the latest values in case the writer blocks
	firstValue := <-watchCh
	latestWatchCh := latestonlychannel.Wrap(watchCh)

	// write the first value first, then loop for the rest
	log.Printf("sending initial counter watch value: %v", firstValue)
	err := stream.Send(&internal_hooks_v1.WatchCounterResponse{
		Value: firstValue,
	})
	if err != nil {
		log.Printf("failed to write watched counter value: %s", err)
		return err
	}
	log.Printf("sent initial counter watch value: %v", firstValue)

	for {
		newValue := <-latestWatchCh

		log.Printf("sending counter watch value: %v", newValue)
		err := stream.Send(&internal_hooks_v1.WatchCounterResponse{
			Value: newValue,
		})
		if err != nil {
			log.Printf("failed to write watched counter value: %s", err)
			return err
		}
		log.Printf("sent counter watch value: %v", newValue)
	}
}
