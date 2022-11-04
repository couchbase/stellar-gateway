package govalcmp

import (
	"encoding/json"

	"github.com/pkg/errors"
)

func boolToUint(val bool) uint64 {
	if val {
		return 1
	} else {
		return 0
	}
}

func jsonToSimpleType(val json.RawMessage) (SimpleType, error) {
	var parsedVal interface{}
	err := json.Unmarshal(val, &parsedVal)
	if err != nil {
		return nil, err
	}

	sval, err := SimplifyValue(parsedVal)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse json to simple type")
	}

	return sval, nil
}
