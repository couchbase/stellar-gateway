package ratelimiting

import (
	"net/http"

	"google.golang.org/grpc"
)

type RateLimiter interface {
	GrpcUnaryInterceptor() grpc.UnaryServerInterceptor
	GrpcStreamInterceptor() grpc.StreamServerInterceptor
	HttpMiddleware(next http.Handler) http.Handler
}
