package main

import (
	"context"
	"crypto/tls"
	"log"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/stellar-gateway/contrib/grpcheaderauth"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	tracer = otel.Tracer("cng-otel-test")
)

func main() {
	connAddr := "192.168.107.2:30813"
	username := "Administrator"
	password := "password"

	ctx := context.Background()

	traceClient := otlptracegrpc.NewClient(
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint("localhost:4317"))
	traceExp, err := otlptrace.New(ctx, traceClient)
	if err != nil {
		panic(err)
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExp)

	traceRes, _ := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceNameKey.String("cng-otel-test"),
	))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(bsp),
		sdktrace.WithResource(traceRes),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

	creds, err := grpcheaderauth.NewGrpcBasicAuth(username, password)
	if err != nil {
		panic(err)
	}

	conn, err := grpc.NewClient(connAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		panic(err)
	}

	kvClient := kv_v1.NewKvServiceClient(conn)

	md := metadata.Pairs(
		"user-id", "some-test-user-id",
	)
	reqCtx := metadata.NewOutgoingContext(context.Background(), md)

	spanCtx, span := tracer.Start(reqCtx, "test-operation")

	resp, err := kvClient.Upsert(spanCtx, &kv_v1.UpsertRequest{
		BucketName:     "default",
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            "test-doc",
		Content: &kv_v1.UpsertRequest_ContentUncompressed{
			ContentUncompressed: []byte(`{"foo": "bar"}`),
		},
	}, grpc.PerRPCCredentials(creds))
	if err != nil {
		panic(err)
	}

	span.End()

	log.Printf("RESP: %+v", resp)
}
