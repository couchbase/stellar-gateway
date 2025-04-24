package testutils

import (
	"os"
	"strconv"
	"testing"
)

type Config struct {
	CbConnStr string
	CbUser    string
	CbPass    string
	SgConnStr string
	SgPort    int
	DapiPort  int
}

var globalTestConfig *Config

func GetTestConfig(t *testing.T) *Config {
	if globalTestConfig == nil {
		testConfig := &Config{
			CbConnStr: "127.0.0.1",
			CbUser:    "Administrator",
			CbPass:    "password",
			SgConnStr: "",
			SgPort:    0,
			DapiPort:  0,
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

		envSgConnStr := os.Getenv("SGTEST_SGCONNSTR")
		if envSgConnStr != "" {
			testConfig.SgConnStr = envSgConnStr
		}

		envSgPort := os.Getenv("SGTEST_SGPORT")
		if envSgPort != "" {
			portNum, err := strconv.Atoi(envSgPort)
			if err != nil {
				t.Fatalf("Failed to parse SG Port: %s", err)
			}
			testConfig.SgPort = portNum
		}

		envDapiPort := os.Getenv("SGTEST_DAPIPORT")
		if envDapiPort != "" {
			portNum, err := strconv.Atoi(envDapiPort)
			if err != nil {
				t.Fatalf("Failed to parse DAPI Port: %s", err)
			}
			testConfig.DapiPort = portNum
		}

		t.Logf("initialized test configuration")
		t.Logf("  cbconnstr: %s", testConfig.CbConnStr)
		t.Logf("  cbuser: %s", testConfig.CbUser)
		t.Logf("  cbpass: %s", testConfig.CbPass)
		t.Logf("  sgconnstr: %s", testConfig.SgConnStr)
		t.Logf("  sgport: %d", testConfig.SgPort)
		t.Logf("  dapiport: %d", testConfig.DapiPort)

		globalTestConfig = testConfig
	}

	return globalTestConfig
}
