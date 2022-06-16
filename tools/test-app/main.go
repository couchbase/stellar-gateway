package main

import (
	"context"
	"flag"
	"io"
	"log"
	"time"

	"github.com/couchbase/stellar-nebula/protos"
	"google.golang.org/grpc/status"

	gocbps "github.com/couchbase/stellar-nebula/test-client"
)

var addr = flag.String("addr", "localhost:18098", "the address to connect to")

func main() {
	flag.Parse()

	client, err := gocbps.Connect(*addr, &gocbps.ConnectOptions{})
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// testing of some routing stuff
	{
		conn := client.GetConn()
		rc := protos.NewRoutingClient(conn)

		wr, err := rc.WatchRouting(ctx, &protos.WatchRoutingRequest{})
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
	}

	// testing some basic CRUD operations

	coll := client.Bucket("default").DefaultCollection()

	upsertRes, err := coll.Upsert(ctx, "hello", []byte(`{"hello": "world"}`), &gocbps.UpsertOptions{})
	if err != nil {
		log.Fatalf("could not upsert: %v", err)
	}
	log.Printf("upsert response: %+v", upsertRes)

	getRes, err := coll.Get(ctx, "hello", &gocbps.GetOptions{})
	if err != nil {
		log.Fatalf("could not get: %v", err)
	}
	log.Printf("get response: %+v", getRes)

	missingGetRes, err := coll.Get(ctx, "i-do-not-exist", &gocbps.GetOptions{})
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
		log.Printf("missing get response: %+v", missingGetRes)
	}

	// testing some query directly via the connection
	{
		conn := client.GetConn()
		cc := protos.NewCouchbaseClient(conn)

		queryResp, err := cc.Query(ctx, &protos.QueryRequest{
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
}
