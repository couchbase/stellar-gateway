package test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"

	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/testutils"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type startedGateway struct {
	createSg bool

	gwConnAddr       string
	gatewayConn      *grpc.ClientConn
	gatewayCloseFunc func()
	gatewayClosedCh  chan struct{}

	dapiCli  *http.Client
	dapiAddr string
}

func startGatewayForTesting(
	ctx context.Context,
	testConfig *testutils.Config,
	logger *zap.Logger,
	grpcCert tls.Certificate,
	dapiCert tls.Certificate,
	clientCaCertPool *x509.CertPool,
	grpcUserAgent string,
) (*startedGateway, error) {
	createSg := testConfig.SgConnStr == ""
	if grpcUserAgent == "" {
		grpcUserAgent = "test-client"
	}

	if createSg {
		gwStartInfoCh := make(chan *gateway.StartupInfo, 1)
		gwCtx, gwCtxCancel := context.WithCancel(ctx)
		gw, err := gateway.NewGateway(&gateway.Config{
			Logger:              logger,
			CbConnStr:           testConfig.CbConnStr,
			Username:            testConfig.CbUser,
			Password:            testConfig.CbPass,
			BoostrapNodeIsLocal: true,
			BindDataPort:        0,
			BindDapiPort:        0,
			GrpcCertificate:     grpcCert,
			DapiCertificate:     dapiCert,
			ClientCaCert:        clientCaCertPool,
			AlphaEndpoints:      true,
			NumInstances:        1,
			ProxyServices:       []string{"query", "analytics", "mgmt", "search"},
			ProxyBlockAdmin:     true,
			Debug:               true,

			StartupCallback: func(m *gateway.StartupInfo) {
				gwStartInfoCh <- m
			},
		})
		if err != nil {
			gwCtxCancel()
			return nil, err
		}

		gwClosedCh := make(chan struct{})
		gwErrCh := make(chan error, 1)
		go func() {
			err := gw.Run(gwCtx)
			if err != nil {
				gwErrCh <- err
			}

			logger.Info("test gateway has shut down")
			close(gwClosedCh)
		}()

		select {
		case startInfo := <-gwStartInfoCh:
			connAddr := fmt.Sprintf("%s:%d", "127.0.0.1", startInfo.ServicePorts.PS)
			dapiAddr := fmt.Sprintf("%s:%d", "127.0.0.1", startInfo.ServicePorts.DAPI)

			conn, err := grpc.NewClient(connAddr,
				grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})),
				grpc.WithUserAgent(grpcUserAgent),
			)
			if err != nil {
				gwCtxCancel()
				return nil, err
			}

			dapiCli := &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				},
			}

			return &startedGateway{
				createSg:         true,
				gwConnAddr:       connAddr,
				gatewayConn:      conn,
				gatewayCloseFunc: gwCtxCancel,
				gatewayClosedCh:  gwClosedCh,
				dapiCli:          dapiCli,
				dapiAddr:         dapiAddr,
			}, nil
		case err := <-gwErrCh:
			gwCtxCancel()
			return nil, err
		case <-gwClosedCh:
			gwCtxCancel()
			return nil, fmt.Errorf("gateway exited before startup")
		case <-ctx.Done():
			gwCtxCancel()
			return nil, ctx.Err()
		}
	}

	connAddr := fmt.Sprintf("%s:%d", testConfig.SgConnStr, testConfig.SgPort)
	conn, err := grpc.NewClient(connAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})),
		grpc.WithUserAgent(grpcUserAgent),
	)
	if err != nil {
		return nil, err
	}

	dapiAddr := fmt.Sprintf("%s:%d", testConfig.SgConnStr, testConfig.DapiPort)
	dapiCli := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	return &startedGateway{
		createSg:    false,
		gwConnAddr:  connAddr,
		gatewayConn: conn,
		dapiCli:     dapiCli,
		dapiAddr:    dapiAddr,
	}, nil
}
