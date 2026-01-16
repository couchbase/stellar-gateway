package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"time"
)

var mode = flag.String("mode", "protostellar", "whether to use protostellar or direct")
var username = flag.String("username", "Administrator", "the username to connect with")
var password = flag.String("password", "password", "the password to connect with")
var addr = flag.String("addr", "localhost", "the address to connect to")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to file")
var mutexprofile = flag.String("mutexprofile", "", "write mutex profile to file")

func main() {
	flag.Parse()

	var wrapper clientWrapper
	switch *mode {
	case "protostellar":
		log.Printf("testing in protostellar mode")
		wrapper = &protostellarWrapper{}
	case "direct":
		log.Printf("testing in direct mode")
		wrapper = &directWrapper{}
	default:
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

	TEST_VALUE_SIZE := 1024
	TEST_VALUE := make([]byte, TEST_VALUE_SIZE)
	for i := 0; i < TEST_VALUE_SIZE; i++ {
		TEST_VALUE[i] = byte(i % 256)
	}

	log.Printf("warming up...")

	// warm up
	WARMUP_COUNT := 20000
	for i := 0; i < WARMUP_COUNT; i++ {
		err := wrapper.Upsert("test-key", TEST_VALUE)
		if err != nil {
			log.Fatalf("warmup upsert failed: %s", err)
		}
	}

	log.Printf("testing time for operations...")

	if cpuprofile != nil && *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Printf("failed to create cpu profile file: %v", err)
			os.Exit(1)
		}

		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Printf("failed to start cpu profiling: %v", err)
			os.Exit(1)
		}

		defer pprof.StopCPUProfile()
	}

	if memprofile != nil && *memprofile != "" {
		defer func() {
			f, err := os.Create(*memprofile)
			if err != nil {
				log.Printf("failed to create memory profile file: %v", err)
				os.Exit(1)
			}

			runtime.GC() // get up-to-date statistics

			err = pprof.Lookup("heap").WriteTo(f, 0)
			if err != nil {
				log.Printf("failed to write memory profile: %v", err)
				os.Exit(1)
			}
		}()
	}

	if mutexprofile != nil && *mutexprofile != "" {
		runtime.SetMutexProfileFraction(1)

		defer func() {
			f, err := os.Create(*mutexprofile)
			if err != nil {
				log.Printf("failed to create mutex profile file: %v", err)
				os.Exit(1)
			}

			err = pprof.Lookup("mutex").WriteTo(f, 0)
			if err != nil {
				log.Printf("failed to write mutex profile: %v", err)
				os.Exit(1)
			}
		}()
	}

	NUM_OPS_TEST := 1000000
	NUM_THREADS := 1024

	var numOpsLeft = int64(NUM_OPS_TEST)
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

				keyIter := prevNumOpsLeft % 2048
				keyName := fmt.Sprintf("test-key-%d", keyIter)

				stime := time.Now()

				err := wrapper.Upsert(keyName, TEST_VALUE)
				if err != nil {
					log.Fatalf("upsert failed: %s", err)
				}

				_, err = wrapper.Get(keyName)
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
	opsPerSec := float64(numOps) / (float64(realTotalOpTime) / float64(time.Second))

	avgOpTime := time.Duration(float64(totalOpTime) / float64(numOps))
	log.Printf("performing %d operations", numOps)
	log.Printf("  took %v", realTotalOpTime)
	log.Printf("  average of %v per op", avgOpTime)
	log.Printf("  %.2f ops per sec", opsPerSec)
}
