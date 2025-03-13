/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package authhdr

import (
	"encoding/base64"
	"strings"
)

func DecodeBasicAuth(hdr string) (string, string, bool) {
	auth := []byte(hdr)

	if auth[0] != 'b' && auth[0] != 'B' {
		return "", "", false
	}
	if auth[1] != 'a' && auth[1] != 'A' {
		return "", "", false
	}
	if auth[2] != 's' && auth[2] != 'S' {
		return "", "", false
	}
	if auth[3] != 'i' && auth[3] != 'I' {
		return "", "", false
	}
	if auth[4] != 'c' && auth[4] != 'C' {
		return "", "", false
	}
	if auth[5] != ' ' {
		return "", "", false
	}

	var decoded []byte
	decLen := base64.StdEncoding.DecodedLen(len(auth) - 6)
	if decLen > 128 {
		dst := make([]byte, decLen)

		n, err := base64.StdEncoding.Decode(dst, auth[6:])
		if err != nil {
			return "", "", false
		}
		decoded = dst[:n]
	} else {
		dst := make([]byte, 128)

		n, err := base64.StdEncoding.Decode(dst, auth[6:])
		if err != nil {
			return "", "", false
		}
		decoded = dst[:n]
	}

	cs := string(decoded)
	username, password, ok := strings.Cut(cs, ":")
	if !ok {
		return "", "", false
	}

	return username, password, true
}
