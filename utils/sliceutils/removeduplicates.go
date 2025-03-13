/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package sliceutils

// RemoveDuplicates removes any duplicate entries from a list.
func RemoveDuplicates[T comparable](in []T) []T {
	dupMap := make(map[T]bool)
	var out []T
	for _, v := range in {
		if _, ok := dupMap[v]; !ok {
			dupMap[v] = true
			out = append(out, v)
		}
	}
	return out
}
