/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package testutils

import (
	"os"
	"testing"
)

type Config struct {
	CbConnStr string
	CbUser    string
	CbPass    string
}

var globalTestConfig *Config

func GetTestConfig(t *testing.T) *Config {
	if globalTestConfig == nil {
		testConfig := &Config{
			CbConnStr: "127.0.0.1",
			CbUser:    "Administrator",
			CbPass:    "password",
		}

		envCbConnStr := os.Getenv("SGTEST_CBCONNSTR")
		if envCbConnStr != "" {
			testConfig.CbConnStr = envCbConnStr
		}

		envCbUser := os.Getenv("SGTEST_CBUSER")
		if envCbUser != "" {
			testConfig.CbUser = envCbUser
		}

		envCbPass := os.Getenv("SGTEST_CBPASS")
		if envCbPass != "" {
			testConfig.CbPass = envCbPass
		}

		t.Logf("initialized test configuration")
		t.Logf("  cbconnstr: %s", testConfig.CbConnStr)
		t.Logf("  cbuser: %s", testConfig.CbUser)
		t.Logf("  cbpass: %s", testConfig.CbPass)

		globalTestConfig = testConfig
	}

	return globalTestConfig
}
