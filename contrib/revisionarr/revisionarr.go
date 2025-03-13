/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// Package revisionarr implements comparison and calculations for a form of
// revision number that is represented by an arbitrarily sized array of uint64's.
// These revision numbers are essentially big endian revisions.
package revisionarr

// IsZero indicates whether this revision is essentially unspecified.
func IsZero(a []uint64) bool {
	for _, v := range a {
		if v != 0 {
			return false
		}
	}

	return true
}

// Compact accepts a revision and compacts away unused parts of the array.
func Compact(a []uint64) []uint64 {
	// if the length is 0, return nil
	if len(a) == 0 {
		return nil
	}

	// while the ending value is 0, remove the ending value
	for len(a) > 0 && a[len(a)-1] == 0 {
		a = a[:len(a)-1]
	}

	// double check we didn't compact it to a zero-length array
	if len(a) == 0 {
		return nil
	}

	return a
}

// Add takes two input revision arrays and returns the result of adding
// each element of each together.  This is useful due to the nature of
// the revisions being monotonically increasing.  You can calculate a
// revision from two underlying revisions simply through addition.
func Add(a, b []uint64) []uint64 {
	// if both inputs are nil, return nil.  If both are zero-length,
	// we return a zero-length result through the logic below.
	if a == nil && b == nil {
		return nil
	}

	var out []uint64

	// The output array needs to be as large as the largest input
	lenA := len(a)
	lenB := len(b)
	if lenA > lenB {
		out = make([]uint64, lenA)
	} else {
		out = make([]uint64, lenB)
	}

	// Scan through both input revisions and add their values to
	// the output revision.
	for elIdx, value := range a {
		out[elIdx] += value
	}
	for elIdx, value := range b {
		out[elIdx] += value
	}

	return out
}

// Compare returns an integer comparing two revisions.  The result will
// be 0 if a == b, -1 if a < b, and +1 if a > b.  A nil argument is
// considered the same as an empty value.
func Compare(a, b []uint64) int {
	lenA := len(a)
	lenB := len(b)

	if lenA > lenB {
		// if a is bigger, we scan all the extra elements, if they are greater
		// than one, this implies a must be larged than b, since those elements
		// in b are considered to be 0.
		for elIdx := lenB; elIdx < lenA; elIdx++ {
			if a[elIdx] > 0 {
				return 1
			}
		}
	} else if lenB > lenA {
		// similar to above, but for b
		for elIdx := lenA; elIdx < lenB; elIdx++ {
			if b[elIdx] > 0 {
				return -1
			}
		}
	}

	// if the miss-matched lengths have not determined the winner at this point
	// we simply need to iterate right-to-left and check who has the larger value.
	var minLen int
	if lenA > lenB {
		minLen = lenB
	} else {
		minLen = lenA
	}

	for invElIdx := 0; invElIdx < minLen; invElIdx++ {
		elIdx := minLen - 1 - invElIdx
		if a[elIdx] > b[elIdx] {
			return +1
		} else if b[elIdx] > a[elIdx] {
			return -1
		}
	}

	// if we make it this far, the values must be identical
	return 0
}
