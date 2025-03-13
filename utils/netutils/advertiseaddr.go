/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package netutils

func GetAdvertiseAddress(bindAddress string) (string, error) {
	// if no advertise port was explicitly provided, use the bind address if it
	// was not an inaddr_any bind.
	if !IsInAddrAny(bindAddress) {
		return bindAddress, nil
	}

	// if the bind address was also not provided, try to get it from the system.
	outboundIP, err := GetOutboundIP()
	if err != nil {
		return "", err
	}

	return outboundIP.String(), nil
}
