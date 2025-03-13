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
