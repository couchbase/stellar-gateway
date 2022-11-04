package govalcmp

import "encoding/json"

// Covers any of the compatible simple types:
// float64, uint64, int64, bool, string, []byte, json.RawMessage
type SimpleType interface{}

type SimpleTypeConstaint interface {
	float64 | uint64 | int64 | bool | string | []byte | json.RawMessage
}

// SimplifyValue coerce all compatible input types to a SimpleType
func SimplifyValue(val interface{}) (SimpleType, error) {
	switch val := val.(type) {
	case float32:
		return float64(val), nil
	case float64:
		return val, nil
	case uint:
		return uint64(val), nil
	case uint8:
		return uint64(val), nil
	case uint16:
		return uint64(val), nil
	case uint32:
		return uint64(val), nil
	case uint64:
		return val, nil
	case int:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case bool:
		return val, nil
	case string:
		return val, nil
	case []byte:
		return val, nil
	case json.RawMessage:
		return val, nil
	}

	// any unsupported type gets converted to JSON
	jsonVal, err := json.Marshal(val)
	if err != nil {
		return nil, err
	}

	return json.RawMessage(jsonVal), nil
}
