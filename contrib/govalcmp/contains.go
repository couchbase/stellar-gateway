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
