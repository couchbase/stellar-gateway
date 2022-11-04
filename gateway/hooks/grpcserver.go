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

func (s *grpcHooksServer) CreateInterceptor(
	ctx context.Context,
	req *internal_hooks_v1.CreateInterceptorRequest,
) (*internal_hooks_v1.CreateInterceptorResponse, error) {
	interceptorID := s.manager.CreateInterceptor()

	return &internal_hooks_v1.CreateInterceptorResponse{
		InterceptorId: interceptorID,
	}, nil
}

func (s *grpcHooksServer) DestroyInterceptor(
	ctx context.Context,
	req *internal_hooks_v1.DestroyInterceptorRequest,
) (*internal_hooks_v1.DestroyInterceptorResponse, error) {
	s.manager.DestroyInterceptor(req.InterceptorId)

	return &internal_hooks_v1.DestroyInterceptorResponse{}, nil
}

func (s *grpcHooksServer) AddHooks(
	ctx context.Context,
	req *internal_hooks_v1.AddHooksRequest,
) (*internal_hooks_v1.AddHooksResponse, error) {
	interceptor := s.manager.GetInterceptor(req.InterceptorId)
	if interceptor == nil {
		return nil, status.Errorf(codes.NotFound, "invalid interceptor id")
	}

	// register all the hooks
	for _, hook := range req.Hooks {
		interceptor.AddHook(hook)
	}

	return &internal_hooks_v1.AddHooksResponse{}, nil
}

func (s *grpcHooksServer) UpdateCounter(
	ctx context.Context,
	req *internal_hooks_v1.UpdateCounterRequest,
) (*internal_hooks_v1.UpdateCounterResponse, error) {
	interceptor := s.manager.GetInterceptor(req.InterceptorId)
	if interceptor == nil {
		return nil, status.Errorf(codes.NotFound, "invalid interceptor id")
	}

	counter := interceptor.GetCounter(req.CounterId)
	newValue := counter.Update(req.Delta)

	return &internal_hooks_v1.UpdateCounterResponse{
		Value: newValue,
	}, nil
}

func (s *grpcHooksServer) WatchCounter(
	req *internal_hooks_v1.WatchCounterRequest,
	stream internal_hooks_v1.Hooks_WatchCounterServer,
) error {
	interceptor := s.manager.GetInterceptor(req.InterceptorId)
	if interceptor == nil {
		return status.Errorf(codes.NotFound, "invalid interceptor id")
	}

	log.Printf("starting counter watcher")

	counter := interceptor.GetCounter(req.CounterId)
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
