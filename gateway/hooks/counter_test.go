/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package hooks

import (
	"context"
	"testing"
)

func TestCounterBasic(t *testing.T) {
	cnt := newCounter()

	initialValue := cnt.Get()
	if initialValue != 0 {
		t.Fatalf("bad initial value")
	}

	watchCtx, watchCancel := context.WithCancel(context.Background())
	watchCh := cnt.Watch(watchCtx)

	// we run the increments in a goroutine so we can watch from the main
	// test goroutine and avoid any additional synchronization

	firstVal := <-watchCh
	if firstVal != 0 {
		t.Fatalf("bad first value")
	}

	go cnt.Update(+1)

	secondVal := <-watchCh
	if secondVal != 1 {
		t.Fatalf("bad second value")
	}

	go cnt.Update(+2)

	thirdVal := <-watchCh
	if thirdVal != 3 {
		t.Fatalf("bad third value")
	}

	watchCancel()
}
