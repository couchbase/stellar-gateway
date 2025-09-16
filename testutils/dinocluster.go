package testutils

import (
	"bufio"
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/couchbase/gocbcorex/cbhttpx"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/ptr"
	"github.com/stretchr/testify/require"
)

var dinoclusterPath = func() string {
	envPath := os.Getenv("DINOCLUSTER_PATH")
	if envPath != "" {
		return envPath
	}

	return "cbdinocluster"
}()

func SkipIfNoDinoCluster(t *testing.T) {
	if globalTestConfig.DinoId == "" {
		t.Skip("skipping due to no dino cluster id")
	}
}

func runDinoCmd(args []string) (string, error) {
	cmd := exec.Command(dinoclusterPath, append([]string{"-v"}, args...)...)
	log.Printf("running command: %s ", strings.Join(cmd.Args, " "))
	log.Printf("---")

	stdOut, _ := cmd.StdoutPipe()
	stdErr, _ := cmd.StderrPipe()

	pipeRdr, pipeWrt := io.Pipe()
	teeRdr := io.TeeReader(stdOut, pipeWrt)

	pipeBufRdr := bufio.NewReader(pipeRdr)
	var output string
	outputWaitCh := make(chan struct{}, 1)
	go func() {
		for {
			line, _, err := pipeBufRdr.ReadLine()
			if err != nil {
				break
			}

			if output != "" {
				output += "\n"
			}
			output += string(line)
		}

		outputWaitCh <- struct{}{}
	}()

	go func() { _, _ = io.Copy(os.Stdout, teeRdr) }()
	go func() { _, _ = io.Copy(os.Stdout, stdErr) }()

	err := cmd.Run()

	_ = pipeWrt.Close()
	<-outputWaitCh

	log.Printf("---")

	return output, err
}

func runNoResDinoCmd(args []string) error {
	_, err := runDinoCmd(args)
	return err
}

func runDinoBlockNodeTraffic(node string) error {
	return runNoResDinoCmd([]string{"chaos", "block-traffic", globalTestConfig.DinoId, node})
}

func runDinoAllowTraffic(node string) error {
	return runNoResDinoCmd([]string{"chaos", "allow-traffic", globalTestConfig.DinoId, node})
}

func runDinoRemoveNode(node string) error {
	return runNoResDinoCmd([]string{"nodes", "rm", globalTestConfig.DinoId, node})
}

type DinoController struct {
	t             *testing.T
	oldFoSettings *cbmgmtx.GetAutoFailoverSettingsResponse
	blockedNodes  []string
}

func StartDinoTesting(t *testing.T, disableAutoFailover bool) *DinoController {
	if globalTestConfig.DinoId == "" {
		t.Error("cannot start dino testing without dino configured")
	}

	c := &DinoController{t: t}
	t.Cleanup(c.cleanup)

	if disableAutoFailover {
		c.DisableAutoFailover()
	}

	return c
}

func (c *DinoController) cleanup() {
	blockedNodes := c.blockedNodes
	c.blockedNodes = nil
	for _, node := range blockedNodes {
		err := runDinoAllowTraffic(node)
		if err != nil {
			c.t.Errorf("failed to reset traffic control for %s", node)
		}
	}

	c.EnableAutoFailover()
}

func getTestMgmt() cbmgmtx.Management {
	return cbmgmtx.Management{
		Transport: http.DefaultTransport,
		UserAgent: "useragent",
		Endpoint:  "http://" + globalTestConfig.CbConnStr + ":8091",
		Auth: &cbhttpx.BasicAuth{
			Username: globalTestConfig.CbUser,
			Password: globalTestConfig.CbPass,
		},
	}
}

func (c *DinoController) DisableAutoFailover() {
	settings, err := getTestMgmt().GetAutoFailoverSettings(context.Background(), &cbmgmtx.GetAutoFailoverSettingsRequest{})
	require.NoError(c.t, err)
	c.oldFoSettings = settings

	err = getTestMgmt().ConfigureAutoFailover(context.Background(), &cbmgmtx.ConfigureAutoFailoverRequest{
		Enabled: ptr.To(false),
	})
	require.NoError(c.t, err)
}

func (c *DinoController) EnableAutoFailover() {
	if c.oldFoSettings == nil {
		return
	}

	err := getTestMgmt().ConfigureAutoFailover(context.Background(), &cbmgmtx.ConfigureAutoFailoverRequest{
		Enabled: ptr.To(c.oldFoSettings.Enabled),
		Timeout: ptr.To(c.oldFoSettings.Timeout),
	})
	require.NoError(c.t, err)
	c.oldFoSettings = nil
}

func (c *DinoController) BlockNodeTraffic(node string) {
	c.blockedNodes = append(c.blockedNodes, node)
	err := runDinoBlockNodeTraffic(node)
	require.NoError(c.t, err)
}

func (c *DinoController) AllowTraffic(node string) {
	err := runDinoAllowTraffic(node)
	require.NoError(c.t, err)
	hostIdx := slices.Index(c.blockedNodes, node)
	if hostIdx >= 0 {
		c.blockedNodes = slices.Delete(c.blockedNodes, hostIdx, hostIdx+1)
	}
}

func (c *DinoController) RemoveNode(node string) {
	err := runDinoRemoveNode(node)
	require.NoError(c.t, err)
}
