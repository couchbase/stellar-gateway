package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/couchbase/stellar-nebula/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var addr = flag.String("addr", "localhost:18098", "the address to connect to")

func main() {
	flag.Parse()
	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	c := protos.NewCouchbaseClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	upsertResp, err := c.Upsert(ctx, &protos.UpsertRequest{
		BucketName:     "default",
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            "hello",
		Content:        []byte(`{"hello": "world"}`),
		ContentType:    protos.DocumentContentType_JSON,
	})
	if err != nil {
		log.Fatalf("could not upsert: %v", err)
	}
	log.Printf("Upsert Response: %+v", upsertResp)

	getResp, err := c.Get(ctx, &protos.GetRequest{
		BucketName:     "default",
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            "hello",
	})
	if err != nil {
		log.Fatalf("could not get: %v", err)
	}
	log.Printf("Get Response: %+v", getResp)

	missingGetResp, err := c.Get(ctx, &protos.GetRequest{
		BucketName:     "default",
		ScopeName:      "_default",
		CollectionName: "_default",
		Key:            "i-do-not-exist",
	})
	if err != nil {
		log.Printf("could not get missing: %v", err)

		if st, ok := status.FromError(err); ok {
			for _, detail := range st.Details() {
				switch typedDetail := detail.(type) {
				case *protos.ErrorInfo:
					log.Printf("error details: %+v", typedDetail)
				}
			}
		}
	} else {
		log.Printf("Get Response: %+v", missingGetResp)
	}
}
