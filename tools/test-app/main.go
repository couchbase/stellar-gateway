package main

import (
	"context"
	"flag"
	"log"
	"time"

	routing_v1 "github.com/couchbase/stellar-nebula/genproto/routing/v1"
	couchbase_v1 "github.com/couchbase/stellar-nebula/genproto/v1"
	"google.golang.org/grpc/status"

	gocbps "github.com/couchbase/stellar-nebula/test-client"
)

var addr = flag.String("addr", "localhost:18098", "the address to connect to")

func main() {
	flag.Parse()

	log.Printf("protostellar test-app starting...")

	client, err := gocbps.Connect(*addr, &gocbps.ConnectOptions{})
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// testing some transactions stuff
	/*
		{
			conn := client.GetConn()
			tc := transactions_v1.NewTransactionsClient(conn)

			testDoc1 := []byte(`{"foo":"baz"}`)
			testDoc2 := []byte(`{"foo":"bar"}`)

			upsertRes, err := client.Bucket("default").DefaultCollection().Upsert(ctx, "test", testDoc1, nil)
			if err != nil {
				log.Fatalf("failed to write test document: %s", err)
			}
			log.Printf("wrote test document: %+v (value: %s)", upsertRes, testDoc1)

			txnBeginResp, err := tc.TransactionBeginAttempt(ctx, &transactions_v1.TransactionBeginAttemptRequest{
				BucketName:    "default",
				TransactionId: nil, // first attempt
			})
			if err != nil {
				log.Fatalf("failed to begin transaction: %s", err)
			}
			log.Printf("began transaction: %+v", txnBeginResp)

			txnGetResp, err := tc.TransactionGet(ctx, &transactions_v1.TransactionGetRequest{
				BucketName:     "default",
				TransactionId:  txnBeginResp.TransactionId,
				AttemptId:      txnBeginResp.AttemptId,
				ScopeName:      "_default",
				CollectionName: "_default",
				Key:            "test",
			})
			if err != nil {
				log.Fatalf("failed to get document in transaction: %s", err)
			}
			log.Printf("got document in transaction: %+v (value: %s)", txnGetResp, txnGetResp.Value)

			txnRepResp, err := tc.TransactionReplace(ctx, &transactions_v1.TransactionReplaceRequest{
				BucketName:     "default",
				TransactionId:  txnBeginResp.TransactionId,
				AttemptId:      txnBeginResp.AttemptId,
				ScopeName:      "_default",
				CollectionName: "_default",
				Key:            "test",
				Value:          testDoc2,
			})
			if err != nil {
				log.Fatalf("failed to replace document in transaction: %s", err)
			}
			log.Printf("replaced document in transaction: %+v (value: %s)", txnRepResp, testDoc2)

			txnCommitResp, err := tc.TransactionCommit(ctx, &transactions_v1.TransactionCommitRequest{
				BucketName:    "default",
				TransactionId: txnBeginResp.TransactionId,
				AttemptId:     txnBeginResp.AttemptId,
			})
			if err != nil {
				log.Fatalf("failed to commit transaction: %s", err)
			}
			log.Printf("committed transaction: %+v", txnCommitResp)

			getRes, err := client.Bucket("default").DefaultCollection().Get(ctx, "test", nil)
			if err != nil {
				log.Fatalf("failed to write test document: %s", err)
			}

			if !bytes.Equal(getRes.Content, testDoc2) {
				log.Fatalf("document content did not match!")
			}

			log.Printf("got updated test document: %+v (value: %s)", getRes, getRes.Content)
		}

		return
	*/

	// testing of some routing stuff
	{
		conn := client.GetConn()
		rc := routing_v1.NewRoutingClient(conn)

		bucketName := "default"
		wr, err := rc.WatchRouting(ctx, &routing_v1.WatchRoutingRequest{
			BucketName: &bucketName,
		})
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

	time.Sleep(10 * time.Second)
	return

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
				case *couchbase_v1.ErrorInfo:
					log.Printf("error details: %+v", typedDetail)
				}
			}
		}
	} else {
		log.Printf("missing get response: %+v", missingGetRes)
	}

	// testing some query directly via the connection
	{
		queryResp, err := client.Query(ctx, "SELECT 1", nil)
		if err != nil {
			log.Fatalf("could not query: %v", err)
		}

		for queryResp.Next() {
			row, err := queryResp.Row()
			if err != nil {
				log.Fatalf("could not query row: %s", err)
			}

			log.Printf("got a query row: %+v", row)
		}

		if err := queryResp.Err(); err != nil {
			log.Fatalf("got a query err: %s", err)
		}

		meta, err := queryResp.MetaData()
		if err != nil {
			log.Fatalf("could not query metadata: %s", err)
		}
		log.Printf("got an query metadata: %+v", meta)
		log.Printf("done streaming query data")
	}

	{
		analyticsResp, err := client.AnalyticsQuery(ctx, "SELECT 1", nil)
		if err != nil {
			log.Fatalf("could not analytics: %s", err)
		}

		for analyticsResp.Next() {
			row, err := analyticsResp.Row()
			if err != nil {
				log.Fatalf("could not analytics row: %s", err)
			}

			log.Printf("got an analytics row: %+v", row)
		}

		if err := analyticsResp.Err(); err != nil {
			log.Fatalf("got an analytics err: %s", err)
		}

		meta, err := analyticsResp.MetaData()
		if err != nil {
			log.Fatalf("could not analytics metadata: %s", err)
		}
		log.Printf("got an analytics metadata: %+v", meta)
		log.Printf("done streaming analytics data")
	}
}
