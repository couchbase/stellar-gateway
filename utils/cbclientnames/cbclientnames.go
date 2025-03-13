/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package cbclientnames

import "strings"

func FromUserAgent(userAgent string) string {
	clientName, _, _ := strings.Cut(userAgent, " ")
	if len(clientName) > 32 {
		clientName = clientName[:32]
	}
	return clientName
}
