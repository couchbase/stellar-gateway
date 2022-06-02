package main

import (
	"context"
	"flag"
	"io"
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wr, err := c.WatchRouting(ctx, &protos.WatchRoutingRequest{})
	if err != nil {
		log.Fatalf("failed to watch routing: %s", err)
	}
	go func() {
		log.Printf("starting to watch routing")
		for {
			routes, err := wr.Recv()
			if err != nil {
				log.Printf("watch routing failed: %s", err)
				break
			}

			log.Printf("got routing: %+v", routes)
		}
	}()

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

	cancelCtx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()
	missingGetResp, err := c.Get(cancelCtx, &protos.GetRequest{
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

	queryResp, err := c.Query(ctx, &protos.QueryRequest{
		Statement: "SELECT 1",
	})
	if err != nil {
		log.Fatalf("could not query: %v", err)
	}

	for {
		queryData, err := queryResp.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Printf("failed to recv query rows: %s", err)
		}

		log.Printf("got some query rows: %+v", queryData)
	}
	log.Printf("done streaming query data")
}
