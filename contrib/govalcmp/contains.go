/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package govalcmp

import "bytes"

func SimpleContains(haystack, needle SimpleType) (bool, error) {
	haystackBytes, err := CoerceSimpleToByteArray(haystack)
	if err != nil {
		return false, err
	}

	needleBytes, err := CoerceSimpleToByteArray(needle)
	if err != nil {
		return false, err
	}

	return bytes.Contains(haystackBytes, needleBytes), nil

}

// Contains checks if needle is in haystack, converting all inputs to []byte
func Contains(haystack interface{}, needle interface{}) (bool, error) {
	shaystack, err := SimplifyValue(haystack)
	if err != nil {
		return false, err
	}

	sneedle, err := SimplifyValue(needle)
	if err != nil {
		return false, err
	}

	return SimpleContains(shaystack, sneedle)
}
