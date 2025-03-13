/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package govalcmp

import (
	"encoding/json"
	"errors"
	"strconv"
)

func CoerceSimpleToSimple[T SimpleTypeConstaint](in SimpleType) (T, error) {
	var tZero T
	switch any(tZero).(type) {
	case float64:
		val, err := CoerceSimpleToFloat(in)
		return any(val).(T), err
	case uint64:
		val, err := CoerceSimpleToUint(in)
		return any(val).(T), err
	case int64:
		val, err := CoerceSimpleToInt(in)
		return any(val).(T), err
	case bool:
		val, err := CoerceSimpleToBool(in)
		return any(val).(T), err
	case string:
		val, err := CoerceSimpleToString(in)
		return any(val).(T), err
	case []byte:
		val, err := CoerceSimpleToByteArray(in)
		return any(val).(T), err
	case json.RawMessage:
		val, err := CoerceSimpleToJson(in)
		return any(val).(T), err
	}

	return tZero, errors.New("unsupported type")
}

func CoerceSimpleToFloat(val SimpleType) (float64, error) {
	switch val := val.(type) {
	case float64:
		return val, nil
	case uint64:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case bool:
		if val {
			return 1, nil
		} else {
			return 0, nil
		}
	case string:
		return strconv.ParseFloat(val, 64)
	case []byte:
		return strconv.ParseFloat(string(val), 64)
	case json.RawMessage:
		sval, err := jsonToSimpleType(val)
		if err != nil {
			return 0, err
		}

		return CoerceSimpleToFloat(sval)
	}

	return 0, errors.New("unsupported type")
}

func CoerceSimpleToUint(val SimpleType) (uint64, error) {
	switch val := val.(type) {
	case float64:
		return uint64(val), nil
	case uint64:
		return val, nil
	case int64:
		return uint64(val), nil
	case bool:
		if val {
			return 1, nil
		} else {
			return 0, nil
		}
	case string:
		return strconv.ParseUint(val, 10, 64)
	case []byte:
		return strconv.ParseUint(string(val), 10, 64)
	case json.RawMessage:
		sval, err := jsonToSimpleType(val)
		if err != nil {
			return 0, err
		}

		return CoerceSimpleToUint(sval)
	}

	return 0, errors.New("unsupported type")
}

func CoerceSimpleToInt(val SimpleType) (int64, error) {
	switch val := val.(type) {
	case float64:
		return int64(val), nil
	case uint64:
		return int64(val), nil
	case int64:
		return val, nil
	case bool:
		if val {
			return 1, nil
		} else {
			return 0, nil
		}
	case string:
		return strconv.ParseInt(val, 10, 64)
	case []byte:
		return strconv.ParseInt(string(val), 10, 64)
	case json.RawMessage:
		sval, err := jsonToSimpleType(val)
		if err != nil {
			return 0, err
		}

		return CoerceSimpleToInt(sval)
	}

	return 0, errors.New("unsupported type")
}

func CoerceSimpleToBool(val SimpleType) (bool, error) {
	switch val := val.(type) {
	case float64:
		return val != 0, nil
	case uint64:
		return val != 0, nil
	case int64:
		return val != 0, nil
	case bool:
		return val, nil
	case string:
		// unsupported
	case []byte:
		// unsupported
	case json.RawMessage:
		sval, err := jsonToSimpleType(val)
		if err != nil {
			return false, err
		}

		return CoerceSimpleToBool(sval)
	}

	return false, errors.New("unsupported type")
}

func CoerceSimpleToString(val SimpleType) (string, error) {
	switch val := val.(type) {
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case uint64:
		return strconv.FormatUint(val, 10), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case bool:
		return strconv.FormatBool(val), nil
	case string:
		return val, nil
	case []byte:
		return string(val), nil
	case json.RawMessage:
		return string(val), nil
	}

	return "", errors.New("unsupported type")
}

func CoerceSimpleToByteArray(val SimpleType) ([]byte, error) {
	switch val := val.(type) {
	case float64:
		return []byte(strconv.FormatFloat(val, 'f', -1, 64)), nil
	case uint64:
		return []byte(strconv.FormatUint(val, 10)), nil
	case int64:
		return []byte(strconv.FormatInt(val, 10)), nil
	case bool:
		return []byte(strconv.FormatBool(val)), nil
	case string:
		return []byte(val), nil
	case []byte:
		return val, nil
	case json.RawMessage:
		return []byte(val), nil
	}

	return nil, errors.New("unsupported type")
}

func CoerceSimpleToJson(val SimpleType) (json.RawMessage, error) {
	return json.Marshal(val)
}
