package main

import (
	"bytes"
	"context"
	"flag"
	"log"
	"time"

	"github.com/couchbase/stellar-nebula/genproto/data_v1"
	"github.com/couchbase/stellar-nebula/genproto/internal_hooks_v1"
	"github.com/couchbase/stellar-nebula/genproto/routing_v1"
	"github.com/couchbase/stellar-nebula/genproto/transactions_v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	gocbps "github.com/couchbase/stellar-nebula/tools/test-client"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
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
				case *epb.ErrorInfo:
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

	// hooks tests
	{
		conn := client.GetConn()
		dc := data_v1.NewDataClient(conn)
		hc := internal_hooks_v1.NewHooksClient(conn)

		hooksContextID := uuid.NewString()

		_, err := hc.CreateHooksContext(ctx, &internal_hooks_v1.CreateHooksContextRequest{
			Id: hooksContextID,
		})
		if err != nil {
			log.Fatalf("could not create hooks context: %s", err)
		}
		log.Printf("created hooks context: %s", hooksContextID)

		// register a hook
		hc.AddHooks(ctx, &internal_hooks_v1.AddHooksRequest{
			HooksContextId: hooksContextID,
			Hooks: []*internal_hooks_v1.Hook{
				{
					Name:         "test",
					Description:  "test description",
					TargetMethod: "/com.couchbase.data.v1.Data/Upsert",
					Actions: []*internal_hooks_v1.HookAction{
						{
							Action: &internal_hooks_v1.HookAction_Counter_{
								Counter: &internal_hooks_v1.HookAction_Counter{
									CounterId: "latch-client",
									Delta:     +1,
								},
							},
						},
						{
							Action: &internal_hooks_v1.HookAction_WaitForCounter_{
								WaitForCounter: &internal_hooks_v1.HookAction_WaitForCounter{
									CounterId: "latch-server",
									Operator:  internal_hooks_v1.ComparisonOperator_EQUAL,
									Value:     1,
								},
							},
						},
					},
				},
			},
		})
		log.Printf("registered hook")

		// start a goroutine waiting for the client latch, and set the server latch in response
		log.Printf("starting latch watcher")
		go func() {
			watchCtx, watchCancel := context.WithCancel(ctx)
			watchSrv, err := hc.WatchCounter(watchCtx, &internal_hooks_v1.WatchCounterRequest{
				HooksContextId: hooksContextID,
				CounterId:      "latch-client",
			})
			if err != nil {
				log.Fatalf("failed to watch counter: %s", err)
			}
			log.Printf("started counter watch")

			for {
				watchResp, err := watchSrv.Recv()
				if err != nil {
					log.Printf("watching failed: %s", err)
					break
				}

				log.Printf("counter watch updated: %v", watchResp.Value)

				if watchResp.Value == 1 {
					// set the server latch
					_, err := hc.UpdateCounter(ctx, &internal_hooks_v1.UpdateCounterRequest{
						HooksContextId: hooksContextID,
						CounterId:      "latch-server",
						Delta:          +1,
					})
					if err != nil {
						log.Fatalf("failed to update counter: %s", err)
					}

					// stop watching
					watchCancel()
				}
			}

			watchCancel()
		}()

		time.Sleep(500 * time.Millisecond)

		// perform the upsert
		log.Printf("executing hooked upsert call")
		upsertCtx := metadata.AppendToOutgoingContext(ctx, "X-Hooks-ID", hooksContextID)
		upsertResp, err := dc.Upsert(upsertCtx, &data_v1.UpsertRequest{
			BucketName:     "default",
			ScopeName:      "",
			CollectionName: "",
			Key:            "test-key",
		})
		if err != nil {
			log.Fatalf("failed to perform hooked upsert: %s", err)
		}

		log.Printf("hooked upsert resp: %+v", upsertResp)

		hc.DestroyHooksContext(ctx, &internal_hooks_v1.DestroyHooksContextRequest{
			Id: hooksContextID,
		})
		log.Printf("destroyed hooks context")
	}
}
