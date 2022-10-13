package revisionarr

import "testing"

func TestBasic(t *testing.T) {
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
