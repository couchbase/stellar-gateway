/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package ratelimiting

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type globalRateState struct {
	MaximumRequests uint64
	Period          time.Duration

	ResetTime time.Time
	Requests  atomic.Uint64
}

type GlobalRateLimiter struct {
	lock  sync.Mutex
	state atomic.Pointer[globalRateState]
}

var _ RateLimiter = (*GlobalRateLimiter)(nil)

func calculateAlignedResetTime(now time.Time, period time.Duration) time.Time {
	// we align the start time to the beginning of the period
	timeMs := now.UnixNano()
	periodMs := int64(period / time.Nanosecond)
	alignedNowMs := (timeMs / periodMs) * periodMs
	alignedNow := time.Unix(0, alignedNowMs)
	resetTime := alignedNow.Add(period)
	return resetTime
}

func NewGlobalRateLimiter(maximumRequests uint64, period time.Duration) *GlobalRateLimiter {
	now := time.Now()
	resetTime := calculateAlignedResetTime(now, period)

	state := &globalRateState{
		Period:          period,
		MaximumRequests: maximumRequests,
		ResetTime:       resetTime,
	}

	limiter := &GlobalRateLimiter{}
	limiter.state.Store(state)

	return limiter
}

func (l *GlobalRateLimiter) getState() *globalRateState {
	state := l.state.Load()

	now := time.Now()
	if !now.Before(state.ResetTime) {
		// we are at the reset time
		l.lock.Lock()
		defer l.lock.Unlock()

		// we need to check again in case another goroutine already updated the state
		state = l.state.Load()
		if now.Before(state.ResetTime) {
			// someone else already reset the state
			return state
		}

		resetTime := calculateAlignedResetTime(now, state.Period)
		state := &globalRateState{
			MaximumRequests: state.MaximumRequests,
			Period:          state.Period,
			ResetTime:       resetTime,
		}
		l.state.Store(state)

		return state
	}

	return state
}

func (l *GlobalRateLimiter) checkAllowed() bool {
	state := l.getState()
	reqNum := state.Requests.Add(1)

	if state.MaximumRequests == 0 {
		return true
	}

	// we use <= rather than < here because reqNum is 1-based
	return reqNum <= uint64(state.MaximumRequests)
}

// SetRateLimit updates the rate limit for this limiter.  Note that this resets
// the rate limit state as part of performing the update.
func (l *GlobalRateLimiter) ResetAndUpdateRateLimit(maximumRequests uint64, period time.Duration) {
	l.lock.Lock()
	defer l.lock.Unlock()

	now := time.Now()
	resetTime := calculateAlignedResetTime(now, period)
	state := &globalRateState{
		MaximumRequests: maximumRequests,
		Period:          period,
		ResetTime:       resetTime,
	}
	l.state.Store(state)
}

func (l *GlobalRateLimiter) GrpcUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if !l.checkAllowed() {
			return nil, status.Errorf(codes.ResourceExhausted, "rate limit exceeded")
		}

		return handler(ctx, req)
	}
}

func (l *GlobalRateLimiter) GrpcStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !l.checkAllowed() {
			return status.Errorf(codes.ResourceExhausted, "rate limit exceeded")
		}

		return handler(srv, ss)
	}
}

func (l *GlobalRateLimiter) HttpMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !l.checkAllowed() {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}
