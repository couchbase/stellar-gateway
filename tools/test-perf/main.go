package main

import (
	"flag"
	"log"
	"sync/atomic"
	"time"
)

var mode = flag.String("mode", "protostellar", "whether to use protostellar or direct")
var username = flag.String("username", "Administrator", "the username to connect with")
var password = flag.String("password", "password", "the password to connect with")
var addr = flag.String("addr", "localhost", "the address to connect to")

func main() {
	flag.Parse()

	var wrapper clientWrapper
	if *mode == "protostellar" {
		log.Printf("testing in protostellar mode")
		wrapper = &protostellarWrapper{}
	} else if *mode == "direct" {
		log.Printf("testing in direct mode")
		wrapper = &directWrapper{}
	} else {
		log.Fatalf("mode must be specified as `protostellar` or `direct`")
	}

	log.Printf("testing time to connect...")

	var totalConnTime time.Duration
	var numConn int64

	NUM_CONNECT_TEST := 10
	for i := 0; i < NUM_CONNECT_TEST; i++ {
		stime := time.Now()

		err := wrapper.Connect(*addr, *username, *password)
		if err != nil {
			log.Fatalf("connect failed: %s", err)
		}

		etime := time.Now()
		connTime := etime.Sub(stime)

		wrapper.Close()

		totalConnTime += connTime
		numConn++
	}

	avgConnTime := time.Duration(int64(totalConnTime) / numConn)
	log.Printf("connecting %d times took %v, with an average of %v per connect", numConn, totalConnTime, avgConnTime)

	err := wrapper.Connect(*addr, *username, *password)
	if err != nil {
		log.Fatalf("op connect failed: %s", err)
	}

	log.Printf("testing time for operations...")

	TEST_VALUE := []byte(`{"str": "hello world, I am a string that is some unknown number of bytes long!"}`)
	NUM_OPS_TEST := 100000
	NUM_THREADS := 64

	var numOpsLeft int64 = int64(NUM_OPS_TEST)
	var numOps int64
	var totalOpTimeInt int64

	tstime := time.Now()

	// start all the threads
	threadWaitCh := make(chan struct{}, NUM_THREADS)
	for i := 0; i < NUM_THREADS; i++ {
		go func() {
			for {
				prevNumOpsLeft := atomic.AddInt64(&numOpsLeft, -2)
				if prevNumOpsLeft < 0 {
					break
				}

				stime := time.Now()

				err := wrapper.Upsert("test-key", TEST_VALUE)
				if err != nil {
					log.Fatalf("upsert failed: %s", err)
				}

				_, err = wrapper.Get("test-key")
				if err != nil {
					log.Fatalf("get failed: %s", err)
				}

				etime := time.Now()
				opTime := etime.Sub(stime)

				atomic.AddInt64(&numOps, 2)
				atomic.AddInt64(&totalOpTimeInt, int64(opTime))
			}

			threadWaitCh <- struct{}{}
		}()
	}

	// wait for all threads to finish
	for i := 0; i < NUM_THREADS; i++ {
		<-threadWaitCh
	}

	tetime := time.Now()
	realTotalOpTime := tetime.Sub(tstime)

	totalOpTime := time.Duration(totalOpTimeInt)

	avgOpTime := time.Duration(int64(totalOpTime) / numOps)
	log.Printf("performing %d operations took %v, with an average of %v per op", numOps, realTotalOpTime, avgOpTime)
}
