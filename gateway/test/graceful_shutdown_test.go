package test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_hooks_v1"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/couchbase/stellar-gateway/utils/selfsignedcert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const shutDownTimeout = 20 * time.Second

func (s *GatewayOpsTestSuite) setupDapiKvGetBarrierHook(ctx context.Context, conn *grpc.ClientConn, hooksContextID, targetPath, barrierID string) {
	hooksClient := internal_hooks_v1.NewHooksServiceClient(conn)

	_, err := hooksClient.CreateHooksContext(ctx, &internal_hooks_v1.CreateHooksContextRequest{
		Id: hooksContextID,
	})
	require.NoError(s.T(), err, "failed to create hooks context")

	_, err = hooksClient.AddHooks(ctx, &internal_hooks_v1.AddHooksRequest{
		HooksContextId: hooksContextID,
		Hooks: []*internal_hooks_v1.Hook{
			{
				Name:         "block-dapi-kv-get",
				Description:  "Block HTTP document GET on a barrier to simulate a long-running request",
				TargetMethod: targetPath,
				Actions: []*internal_hooks_v1.HookAction{
					{
						Action: &internal_hooks_v1.HookAction_WaitOnBarrier_{
							WaitOnBarrier: &internal_hooks_v1.HookAction_WaitOnBarrier{
								BarrierId: barrierID,
							},
						},
					},
					{
						Action: &internal_hooks_v1.HookAction_Execute_{
							Execute: &internal_hooks_v1.HookAction_Execute{},
						},
					},
				},
			},
		},
	})
	require.NoError(s.T(), err, "failed to add hooks")
}

func (s *GatewayOpsTestSuite) setupKvGetBarrierHook(ctx context.Context, conn *grpc.ClientConn, hooksContextID, barrierID string) {
	hooksClient := internal_hooks_v1.NewHooksServiceClient(conn)

	_, err := hooksClient.CreateHooksContext(ctx, &internal_hooks_v1.CreateHooksContextRequest{
		Id: hooksContextID,
	})
	require.NoError(s.T(), err, "failed to create hooks context")

	_, err = hooksClient.AddHooks(ctx, &internal_hooks_v1.AddHooksRequest{
		HooksContextId: hooksContextID,
		Hooks: []*internal_hooks_v1.Hook{
			{
				Name:         "block-kv-get",
				Description:  "Block KV Get on a barrier to simulate a long-running request",
				TargetMethod: "/couchbase.kv.v1.KvService/Get",
				Actions: []*internal_hooks_v1.HookAction{
					{
						Action: &internal_hooks_v1.HookAction_WaitOnBarrier_{
							WaitOnBarrier: &internal_hooks_v1.HookAction_WaitOnBarrier{
								BarrierId: barrierID,
							},
						},
					},
					{
						Action: &internal_hooks_v1.HookAction_Execute_{
							Execute: &internal_hooks_v1.HookAction_Execute{},
						},
					},
				},
			},
		},
	})
	require.NoError(s.T(), err, "failed to add hooks")
}

func (s *GatewayOpsTestSuite) TestGracefulShutdown() {
	s.T().Logf("setting up new instance of stellar gateway...")

	gwCert, err := selfsignedcert.GenerateCertificate()
	if err != nil {
		s.T().Fatalf("failed to create testing certificate: %s", err)
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		s.T().Fatalf("failed to initialize test logging: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	testConfig := testutils.GetTestConfig(s.T())

	gwStartInfoCh := make(chan *gateway.StartupInfo, 1)
	gw, err := gateway.NewGateway(&gateway.Config{
		Logger:          logger.Named("graceful-shutdown-gateway"),
		CbConnStr:       testConfig.CbConnStr,
		Username:        testConfig.CbUser,
		Password:        testConfig.CbPass,
		GrpcCertificate: *gwCert,
		DapiCertificate: *gwCert,
		NumInstances:    1,
		ProxyServices:   []string{"query"},
		Debug:           true,
		StartupCallback: func(m *gateway.StartupInfo) {
			gwStartInfoCh <- m
		},
		ShutdownTimeout: shutDownTimeout,
	})
	if err != nil {
		s.T().Fatalf("failed to initialize graceful-shutdown-gateway: %s", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := gw.Run(ctx)
		if err != nil {
			s.T().Errorf("graceful-shutdown-gateway run failed: %s", err)
		}

		s.T().Logf("graceful-shutdown-gateway has shut down")
	}()

	startInfo := <-gwStartInfoCh

	dapiAddr := fmt.Sprintf("%s:%d", "127.0.0.1", startInfo.ServicePorts.DAPI)
	dapiCli := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	connAddr := fmt.Sprintf("%s:%d", "127.0.0.1", startInfo.ServicePorts.PS)
	conn, err := grpc.NewClient(connAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})),
		grpc.WithUserAgent("test-client"),
	)
	require.NoError(s.T(), err, "failed to create grpc client connection to gateway")
	defer func() { _ = conn.Close() }()

	testKey := s.testDocId()

	docPath := fmt.Sprintf("/v1/buckets/%s/scopes/%s/collections/%s/documents/%s", s.bucketName, s.scopeName, s.collectionName, testKey)

	eofDapiHooksContextID := fmt.Sprintf("http-eof-%s", testKey)
	s.setupDapiKvGetBarrierHook(ctx, conn, eofDapiHooksContextID, docPath, "eof-barrier")

	successDapiHooksContextID := fmt.Sprintf("http-success-%s", testKey)
	s.setupDapiKvGetBarrierHook(ctx, conn, successDapiHooksContextID, docPath, "success-barrier")

	eofGrpcHooksContextID := fmt.Sprintf("grpc-eof-%s", testKey)
	s.setupKvGetBarrierHook(ctx, conn, eofGrpcHooksContextID, "eof-barrier")

	successGrpcHooksContextID := fmt.Sprintf("grpc-success-%s", testKey)
	s.setupKvGetBarrierHook(ctx, conn, successGrpcHooksContextID, "temp-barrier")

	// Set up request watchers so we know when the server starts processing each request.
	eofDapiWatch := startInfo.HooksManager.GetHooksContext(eofDapiHooksContextID).WatchRequests(ctx)
	successDapiWatch := startInfo.HooksManager.GetHooksContext(successDapiHooksContextID).WatchRequests(ctx)
	eofGrpcWatch := startInfo.HooksManager.GetHooksContext(eofGrpcHooksContextID).WatchRequests(ctx)
	successGrpcWatch := startInfo.HooksManager.GetHooksContext(successGrpcHooksContextID).WatchRequests(ctx)

	// Check that requests to Data API taking longer than the shutdown timeout will eventually be forcibly closed.
	wg.Add(1)
	go func() {
		defer wg.Done()

		req, err := http.NewRequest("GET", fmt.Sprintf("https://%s%s", dapiAddr, docPath), nil)
		require.NoError(s.T(), err, "failed to create http eof request")

		req.SetBasicAuth(testConfig.CbUser, testConfig.CbPass)
		req.Header.Set("X-Hooks-ID", eofDapiHooksContextID)

		_, err = dapiCli.Do(req)

		// After the shutdown timeout has elapsed the gateway should forcibly close connections, causing the client to see an
		// EOF error.
		assert.ErrorIs(s.T(), err, io.EOF)
	}()

	// Check that requests that start before shutdown is called but take less than the shutdown timeout complete successfully.
	wg.Add(1)
	go func() {
		defer wg.Done()

		req, err := http.NewRequest("GET", fmt.Sprintf("https://%s%s", dapiAddr, docPath), nil)
		require.NoError(s.T(), err, "failed to create http success request")

		req.SetBasicAuth(testConfig.CbUser, testConfig.CbPass)
		req.Header.Set("X-Hooks-ID", successDapiHooksContextID)

		resp, err := dapiCli.Do(req)

		assert.NoError(s.T(), err, "expected request to complete successfully before shutdown timeout")
		if err != nil {
			return
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	// Check that grpc requests that take longer than the shutdown timeout will eventually be forcibly closed.
	wg.Add(1)
	go func() {
		defer wg.Done()

		kvClient := kv_v1.NewKvServiceClient(conn)

		grpcCtx := metadata.AppendToOutgoingContext(ctx, "X-Hooks-ID", eofGrpcHooksContextID)
		_, err := kvClient.Get(grpcCtx, &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            testKey,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))

		assert.Error(s.T(), err, "expected error from grpc request after forcible shutdown")

		st, ok := status.FromError(err)
		assert.True(s.T(), ok)
		assert.Equal(s.T(), codes.Unavailable, st.Code())
	}()

	// Signal the barriers after a delay to allow requests to be in-flight when shutdown is called, but unblock them
	// before the shutdown timeout elapses so that they can complete successfully.
	time.AfterFunc(shutDownTimeout-time.Second*5, func() {
		hooksCtx := startInfo.HooksManager.GetHooksContext(successGrpcHooksContextID)
		if hooksCtx != nil {
			hooksCtx.GetBarrier("temp-barrier").SignalAll(nil)
		}

		httpHooksCtx := startInfo.HooksManager.GetHooksContext(successDapiHooksContextID)
		if httpHooksCtx != nil {
			httpHooksCtx.GetBarrier("success-barrier").SignalAll(nil)
		}
	})

	wg.Add(1)
	go func() {
		defer wg.Done()

		kvClient := kv_v1.NewKvServiceClient(conn)

		grpcCtx := metadata.AppendToOutgoingContext(ctx, "X-Hooks-ID", successGrpcHooksContextID)
		_, err := kvClient.Get(grpcCtx, &kv_v1.GetRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            testKey,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		assert.NoError(s.T(), err, "expected grpc request that starts before shutdown to complete successfully")
	}()

	// Wait for the server to start processing all four requests before initiating shutdown.
	<-eofDapiWatch
	<-successDapiWatch
	<-eofGrpcWatch
	<-successGrpcWatch

	gw.Shutdown()

	wg.Wait()
}
