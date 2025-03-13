/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package latestonlychannel

import (
	"testing"
	"time"
)

func TestLatestOnlyChannel_EmptyBlock(t *testing.T) {
	inputCh := make(chan int)
	outputCh := Wrap(inputCh)

	select {
	case <-outputCh:
		t.Fatalf("should have blocked")
	case <-time.After(10 * time.Millisecond):
	}

	close(inputCh)
}

func TestLatestOnlyChannel_Single(t *testing.T) {
	inputCh := make(chan int)
	outputCh := Wrap(inputCh)

	// not waiting works because although these channels are unbuffered,
	// the deduplication logic acts as if its a buffered 1-length channel.

	inputCh <- 1
	recvNum := <-outputCh
	if recvNum != 1 {
		t.Fatalf("unexpected recv number")
	}

	inputCh <- 2
	recvNum = <-outputCh
	if recvNum != 2 {
		t.Fatalf("unexpected recv number")
	}

	close(inputCh)

	_, ok := <-outputCh
	if ok {
		t.Fatalf("output channel was not closed")
	}
}

func TestLatestOnlyChannel_Multiple(t *testing.T) {
	inputCh := make(chan int)
	outputCh := Wrap(inputCh)

	inputCh <- 1
	inputCh <- 2
	inputCh <- 3
	recvNum := <-outputCh
	if recvNum != 3 {
		t.Fatalf("unexpected recv number")
	}

	inputCh <- 4
	inputCh <- 5
	inputCh <- 6
	recvNum = <-outputCh
	if recvNum != 6 {
		t.Fatalf("unexpected recv number")
	}

	close(inputCh)

	_, ok := <-outputCh
	if ok {
		t.Fatalf("output channel was not closed")
	}
}
