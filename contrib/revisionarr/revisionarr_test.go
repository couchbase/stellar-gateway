package revisionarr

import (
	"reflect"
	"testing"
)

func TestIsZero(t *testing.T) {
	checkOne := func(a []uint64, e bool) {
		c := IsZero(a)
		if c != e {
			t.Fatalf("unexpected result for IsZero(%v), yielded %t instead of %t", a, c, e)
		}
	}

	checkOne(nil, true)
	checkOne([]uint64{}, true)
	checkOne([]uint64{0}, true)
	checkOne([]uint64{0, 0, 0, 0, 0}, true)
	checkOne([]uint64{1}, false)
	checkOne([]uint64{1, 0, 0, 0, 0}, false)
	checkOne([]uint64{0, 0, 0, 0, 1}, false)
}

func TestCompact(t *testing.T) {
	checkOne := func(a []uint64, e []uint64) {
		c := Compact(a)
		if !reflect.DeepEqual(c, e) {
			t.Fatalf("unexpected result for Compact(%v), yielded %v instead of %v", a, c, e)
		}
	}

	checkOne([]uint64{0}, nil)
	checkOne([]uint64{0, 1}, []uint64{0, 1})
	checkOne([]uint64{1, 0}, []uint64{1})
	checkOne([]uint64{0, 0, 0, 0, 0}, nil)
	checkOne([]uint64{0, 0, 0, 0, 1}, []uint64{0, 0, 0, 0, 1})
	checkOne([]uint64{1, 0, 0, 0, 0}, []uint64{1})
	checkOne([]uint64{1, 2, 3, 4, 5}, []uint64{1, 2, 3, 4, 5})
}

func TestAdd(t *testing.T) {
	checkOne := func(a []uint64, b []uint64, e []uint64) {
		c := Add(a, b)
		if !reflect.DeepEqual(c, e) {
			t.Fatalf("unexpected result for Add(%v, %v), yielded %v instead of %v", a, b, c, e)
		}

		// addition should be order invariant, so check the reverse
		ic := Add(b, a)
		if !reflect.DeepEqual(ic, e) {
			t.Fatalf("unexpected inverse result for Add(%v, %v), yielded %v instead of %v", b, a, c, e)
		}
	}

	checkOne(nil, nil, nil)
	checkOne([]uint64{}, nil, []uint64{})
	checkOne([]uint64{}, []uint64{}, []uint64{})
	checkOne([]uint64{0}, []uint64{0}, []uint64{0})
	checkOne([]uint64{1}, []uint64{1}, []uint64{2})
	checkOne([]uint64{1, 0}, []uint64{1, 0}, []uint64{2, 0})
	checkOne([]uint64{0, 1}, []uint64{0, 1}, []uint64{0, 2})
	checkOne([]uint64{1, 1}, []uint64{1, 1}, []uint64{2, 2})
	checkOne([]uint64{5, 4, 3, 2, 1}, []uint64{10, 9, 8, 7, 6}, []uint64{15, 13, 11, 9, 7})
	checkOne([]uint64{1, 2, 3, 4, 5}, []uint64{8, 9}, []uint64{9, 11, 3, 4, 5})

}

func TestCompare(t *testing.T) {
	checkOne := func(a, b []uint64, e int) {
		c := Compare(a, b)
		if c != e {
			t.Fatalf("unexpected result for Compare(%v, %v), yielded %d instead of %d", a, b, c, e)
		}

		// we also check that the inversion matches
		var ie int
		if e == +1 {
			ie = -1
		} else if e == -1 {
			ie = +1
		} else {
			ie = 0
		}

		ic := Compare(b, a)
		if ic != ie {
			t.Fatalf("unexpected inverse result for Compare(%v, %v), yielded %d instead of %d", b, a, c, e)
		}
	}

	// TODO(brett19): Add more of the possibilities here.

	checkOne(nil, nil, 0)
	checkOne([]uint64{}, nil, 0)
	checkOne([]uint64{}, []uint64{}, 0)
	checkOne([]uint64{0}, []uint64{0}, 0)
	checkOne([]uint64{1}, []uint64{1}, 0)
	checkOne([]uint64{0, 0, 0, 0, 0}, []uint64{0, 0, 0, 0, 0}, 0)
	checkOne([]uint64{1, 1, 1, 1, 1}, []uint64{1, 1, 1, 1, 1}, 0)
	checkOne([]uint64{0, 0, 0, 0, 0}, []uint64{0}, 0)
	checkOne([]uint64{1, 1, 1, 1, 1}, []uint64{0}, 1)
	checkOne([]uint64{1, 2, 3, 4, 5}, []uint64{5, 4, 3, 2, 1}, 1)
	checkOne([]uint64{0, 0, 0, 1}, []uint64{0, 0, 0}, 1)
}
