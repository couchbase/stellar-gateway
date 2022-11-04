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
