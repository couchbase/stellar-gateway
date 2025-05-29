package test

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/goprotostellar/genproto/admin_collection_v1"
	"github.com/golang/snappy"
	"golang.org/x/mod/semver"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/stellar-gateway/contrib/grpcheaderauth"
	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/couchbase/stellar-gateway/utils/selfsignedcert"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

type GatewayOpsTestSuite struct {
	suite.Suite

	testClusterInfo  *testutils.CanonicalTestCluster
	gatewayCloseFunc func()
	gatewayConn      *grpc.ClientConn
	gatewayClosedCh  chan struct{}
	dapiCli          *http.Client
	dapiAddr         string

	bucketName     string
	scopeName      string
	collectionName string
	badRpcCreds    credentials.PerRPCCredentials
	basicRpcCreds  credentials.PerRPCCredentials
	readRpcCreds   credentials.PerRPCCredentials

	badRestCreds   string
	basicRestCreds string
	readRestCreds  string

	clusterVersion *NodeVersion
	features       []TestFeature

	createSg bool
}

func (s *GatewayOpsTestSuite) incorrectCas(validCas uint64) uint64 {
	// We need to be a bit clever to avoid accidentally using one of the special
	// CAS values at the top or bottom end (0x00000000 and 0xFFFFFFFF are reserved)
	if validCas > 65535 {
		return validCas - 1
	} else {
		return validCas + 1
	}
}

func (s *GatewayOpsTestSuite) randomDocId() string {
	return "test-doc" + "_" + uuid.NewString()
}

func (s *GatewayOpsTestSuite) testDocIdAndCas() (string, uint64) {
	docId := s.randomDocId()
	docCas := s.createDocument(createDocumentOptions{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		DocId:          docId,
		Content:        TEST_CONTENT,
		ContentFlags:   TEST_CONTENT_FLAGS,
	})
	return docId, docCas
}

func (s *GatewayOpsTestSuite) testDocId() string {
	docId, _ := s.testDocIdAndCas()
	return docId
}

func (s *GatewayOpsTestSuite) binaryDocId(content []byte) string {
	docId := s.randomDocId()
	s.createDocument(createDocumentOptions{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		DocId:          docId,
		Content:        content,
		ContentFlags:   0,
	})
	return docId
}

func (s *GatewayOpsTestSuite) tooDeepJson() []byte {
	type testObj struct {
		A interface{} `json:"a"`
	}
	var makeDeepObj func(int) interface{}
	makeDeepObj = func(depth int) interface{} {
		if depth <= 0 {
			return 4
		}
		return &testObj{
			A: makeDeepObj(depth - 1),
		}
	}
	deepObj := makeDeepObj(48)
	deepObjBytes, _ := json.Marshal(deepObj)
	return deepObjBytes
}

func (s *GatewayOpsTestSuite) jsonPathOfDepth(depth int) string {
	var longPathParts []string
	for i := 0; i < depth; i++ {
		longPathParts = append(longPathParts, fmt.Sprintf("%c", 'a'+(i%26)))
	}
	return strings.Join(longPathParts, ".")
}

func (s *GatewayOpsTestSuite) jsonPathOfLen(len int) string {
	var longPath string
	for i := 0; i < len; i++ {
		longPath = longPath + fmt.Sprintf("%c", 'a'+(i%26))
	}
	return longPath
}

func (s *GatewayOpsTestSuite) lockDoc(docId string) {
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)
	galResp, err := kvClient.GetAndLock(context.Background(), &kv_v1.GetAndLockRequest{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		Key:            docId,
		LockTime:       30,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), galResp, err)
	assertValidCas(s.T(), galResp.Cas)
}

func (s *GatewayOpsTestSuite) lockedDocId() string {
	docId := s.testDocId()
	s.lockDoc(docId)
	return docId
}

func (s *GatewayOpsTestSuite) missingDocId() string {
	return s.randomDocId()
}

func (s *GatewayOpsTestSuite) docIdOfLen(len int) string {
	tooLongIdParts := []string{}
	for i := 0; i < len; i++ {
		tooLongIdParts = append(tooLongIdParts, fmt.Sprintf("%c", 'a'+(i%26)))
	}
	return strings.Join(tooLongIdParts, "")
}

func (s *GatewayOpsTestSuite) loadTestData(path string) []byte {
	b, err := os.ReadFile(path)
	s.NoErrorf(err, "Failed to read test data for %s", path)

	return b
}

func (s *GatewayOpsTestSuite) makeRestBasicCreds(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(
		username+":"+password,
	))
}

func (s *GatewayOpsTestSuite) SetupSuite() {
	s.T().Logf("setting up gateway ops suite")

	testConfig := testutils.GetTestConfig(s.T())

	s.createSg = testConfig.SgConnStr == ""

	s.T().Logf("setting up canonical test cluster...")

	testClusterInfo, err := testutils.SetupCanonicalTestCluster(testutils.CanonicalTestClusterOptions{
		ConnStr:  testConfig.CbConnStr,
		Username: testConfig.CbUser,
		Password: testConfig.CbPass,
	})
	if err != nil {
		s.T().Fatalf("failed to setup canonical test cluster: %s", err)
	}

	s.T().Logf("canonical test cluster ready...")

	badRpcCreds, err := grpcheaderauth.NewGrpcBasicAuth(
		"invalid-user", "invalid-pass")
	if err != nil {
		s.T().Fatalf("failed to setup basic bad auth")
	}

	basicRpcCreds, err := grpcheaderauth.NewGrpcBasicAuth(
		testClusterInfo.BasicUser, testClusterInfo.BasicPass)
	if err != nil {
		s.T().Fatalf("failed to setup basic basic auth")
	}

	readRpcCreds, err := grpcheaderauth.NewGrpcBasicAuth(
		testClusterInfo.ReadUser, testClusterInfo.ReadPass)
	if err != nil {
		s.T().Fatalf("failed to setup basic read auth")
	}

	s.testClusterInfo = testClusterInfo
	s.bucketName = testClusterInfo.BucketName
	s.scopeName = testClusterInfo.ScopeName
	s.collectionName = testClusterInfo.CollectionName
	s.badRpcCreds = badRpcCreds
	s.basicRpcCreds = basicRpcCreds
	s.readRpcCreds = readRpcCreds
	s.badRestCreds = s.makeRestBasicCreds("invalid-user", "invalid-pass")
	s.basicRestCreds = s.makeRestBasicCreds(testClusterInfo.BasicUser, testClusterInfo.BasicPass)
	s.readRestCreds = s.makeRestBasicCreds(testClusterInfo.ReadUser, testClusterInfo.ReadPass)

	clusterVersionStr := os.Getenv("SGTEST_CLUSTER_VER")
	if clusterVersionStr == "" {
		clusterVersionStr = DefaultClusterVer
	}
	s.clusterVersion = s.nodeVersionFromString(clusterVersionStr)

	s.ParseSupportedFeatures(os.Getenv("SGTEST_FEATS"))

	if s.createSg {
		s.T().Logf("launching test gateway...")

		logger, err := zap.NewDevelopment()
		if err != nil {
			s.T().Fatalf("failed to initialize test logging: %s", err)
		}

		gwCert, err := selfsignedcert.GenerateCertificate()
		if err != nil {
			s.T().Fatalf("failed to create testing certificate: %s", err)
		}

		gwStartInfoCh := make(chan *gateway.StartupInfo, 1)
		gwCtx, gwCtxCancel := context.WithCancel(context.Background())
		gw, err := gateway.NewGateway(&gateway.Config{
			Logger:          logger.Named("gateway"),
			CbConnStr:       testConfig.CbConnStr,
			Username:        testConfig.CbUser,
			Password:        testConfig.CbPass,
			BindDataPort:    0,
			BindSdPort:      0,
			BindDapiPort:    0,
			GrpcCertificate: *gwCert,
			DapiCertificate: *gwCert,
			AlphaEndpoints:  true,
			NumInstances:    1,
			ProxyServices:   []string{"query", "analytics", "mgmt", "search"},

			StartupCallback: func(m *gateway.StartupInfo) {
				gwStartInfoCh <- m
			},
		})
		if err != nil {
			s.T().Fatalf("failed to initialize gateway: %s", err)
		}

		gwClosedCh := make(chan struct{})
		go func() {
			err := gw.Run(gwCtx)
			if err != nil {
				s.T().Errorf("gateway run failed: %s", err)
			}

			s.T().Logf("test gateway has shut down")
			close(gwClosedCh)
		}()

		startInfo := <-gwStartInfoCh

		connAddr := fmt.Sprintf("%s:%d", "127.0.0.1", startInfo.AdvertisePorts.PS)
		conn, err := grpc.NewClient(connAddr,
			grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
				InsecureSkipVerify: true,
			})), grpc.WithUserAgent("test-client"))
		if err != nil {
			s.T().Fatalf("failed to connect to test gateway: %s", err)
		}

		dapiAddr := fmt.Sprintf("%s:%d", "127.0.0.1", startInfo.AdvertisePorts.DAPI)
		dapiCli := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}

		s.gatewayConn = conn
		s.gatewayCloseFunc = gwCtxCancel
		s.gatewayClosedCh = gwClosedCh
		s.dapiCli = dapiCli
		s.dapiAddr = dapiAddr
	} else {
		connAddr := fmt.Sprintf("%s:%d", testConfig.SgConnStr, testConfig.SgPort)
		conn, err := grpc.NewClient(connAddr,
			grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
				InsecureSkipVerify: true,
			})), grpc.WithUserAgent("test-client"))
		if err != nil {
			s.T().Fatalf("failed to connect to test gateway: %s", err)
		}

		dapiAddr := fmt.Sprintf("%s:%d", testConfig.SgConnStr, testConfig.DapiPort)
		dapiCli := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}

		s.gatewayConn = conn
		s.dapiCli = dapiCli
		s.dapiAddr = dapiAddr
	}
}

func (s *GatewayOpsTestSuite) TearDownSuite() {
	if s.createSg {
		s.T().Logf("tearing down gateway ops suite")

		s.gatewayCloseFunc()
		<-s.gatewayClosedCh
		s.gatewayConn = nil

		s.dapiCli.CloseIdleConnections()
		s.dapiCli = nil
	}
}

func (s *GatewayOpsTestSuite) ParseSupportedFeatures(featsStr string) {
	s.features = []TestFeature{}

	if featsStr == "" {
		return
	}

	featStrs := strings.Split(featsStr, ",")
	for _, featStr := range featStrs {
		featStr = strings.TrimSpace(featStr)
		feat := TestFeatureCode(strings.TrimLeft(featStr, "+-"))

		enabled := true
		if strings.HasPrefix(featStr, "-") {
			enabled = false
		}

		s.features = append(s.features, TestFeature{
			Code:    feat,
			Enabled: enabled,
		})
	}
}

var TEST_CONTENT = []byte(`{"foo": "bar","obj":{"num":14,"arr":[2,5,8],"str":"zz"},"num":11,"arr":[3,6,9,12]}`)
var TEST_CONTENT_FLAGS = uint32(0x01000000)

func (s *GatewayOpsTestSuite) compressContent(in []byte) []byte {
	if in == nil {
		return nil
	}

	out := make([]byte, snappy.MaxEncodedLen(len(in)))
	out = snappy.Encode(out, in)
	return out
}

func (s *GatewayOpsTestSuite) decompressContent(in []byte) []byte {
	if in == nil {
		return nil
	}

	out := make([]byte, len(in))
	out, err := snappy.Decode(out, in)
	require.NoError(s.T(), err)
	return out
}

func (s *GatewayOpsTestSuite) largeTestContent() []byte {
	var v []byte
	for i := 0; i < 21000000; i++ {
		v = append(v, byte(i))
	}

	return v
}

func (s *GatewayOpsTestSuite) largeTestRandomContent() []byte {
	var v []byte
	for i := 0; i < 21000000; i++ {
		v = append(v, byte(rand.Intn(2100000)))
	}

	return v
}

type createDocumentOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	DocId          string
	Content        []byte
	ContentFlags   uint32
}

func (s *GatewayOpsTestSuite) createDocument(opts createDocumentOptions) uint64 {
	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	upsertResp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
		BucketName:     opts.BucketName,
		ScopeName:      opts.ScopeName,
		CollectionName: opts.CollectionName,
		Key:            opts.DocId,
		Content: &kv_v1.UpsertRequest_ContentUncompressed{
			ContentUncompressed: opts.Content,
		},
		ContentFlags: opts.ContentFlags,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), upsertResp, err)
	assertValidCas(s.T(), upsertResp.Cas)
	assertValidMutationToken(s.T(), upsertResp.MutationToken, s.bucketName)

	return upsertResp.Cas
}

type expiryCheckType int

const (
	expiryCheckType_None expiryCheckType = iota
	expiryCheckType_Set
	expiryCheckType_Past
	expiryCheckType_Future
	expiryCheckType_Within
)

type expiryCheckTypeWithinBounds struct {
	MinSecs int
	MaxSecs int
}

type checkDocumentOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	DocId          string
	Content        []byte
	ContentFlags   uint32
	CheckAsJson    bool

	expiry       expiryCheckType
	expiryBounds expiryCheckTypeWithinBounds
}

func (o checkDocumentOptions) ApplyMissingDefaults() checkDocumentOptions {
	return o
}

func (o checkDocumentOptions) ExpectExpiry() checkDocumentOptions {
	o.expiry = expiryCheckType_Set
	return o
}

func (o checkDocumentOptions) ExpectPastExpiry() checkDocumentOptions {
	o.expiry = expiryCheckType_Future
	return o
}

func (o checkDocumentOptions) ExpectFutureExpiry() checkDocumentOptions {
	o.expiry = expiryCheckType_Future
	return o
}

func (s *GatewayOpsTestSuite) checkDocument(t *testing.T, opts checkDocumentOptions) {
	opts = opts.ApplyMissingDefaults()

	kvClient := kv_v1.NewKvServiceClient(s.gatewayConn)

	getResp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
		BucketName:     opts.BucketName,
		ScopeName:      opts.ScopeName,
		CollectionName: opts.CollectionName,
		Key:            opts.DocId,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))

	// nil content means we expect the document not to exist...
	if opts.Content == nil {
		assertRpcStatus(s.T(), err, codes.NotFound)
		return
	}

	// otherwise we assume it exists and apply the rules
	requireRpcSuccess(s.T(), getResp, err)
	assertValidCas(s.T(), getResp.Cas)

	if !opts.CheckAsJson {
		assert.Equal(s.T(), opts.Content, getResp.GetContentUncompressed())
	} else {
		assert.JSONEq(s.T(), string(opts.Content), string(getResp.GetContentUncompressed()))
	}
	assert.Equal(s.T(), opts.ContentFlags, getResp.ContentFlags)

	switch opts.expiry {
	case expiryCheckType_None:
		assert.Nil(s.T(), getResp.Expiry)
	case expiryCheckType_Set:
		requireValidTimestamp(s.T(), getResp.Expiry)
	case expiryCheckType_Future:
		requireValidTimestamp(s.T(), getResp.Expiry)
		ts := getResp.Expiry.AsTime()
		assert.True(t, !ts.Before(time.Now()))
	case expiryCheckType_Past:
		requireValidTimestamp(s.T(), getResp.Expiry)
		ts := getResp.Expiry.AsTime()
		assert.True(t, !ts.After(time.Now()))
	case expiryCheckType_Within:
		requireValidTimestamp(s.T(), getResp.Expiry)
		expiryTime := time.Unix(getResp.Expiry.Seconds, int64(getResp.Expiry.Nanos))
		expirySecs := int(time.Until(expiryTime) / time.Second)
		assert.Greater(s.T(), expirySecs, opts.expiryBounds.MinSecs)
		assert.LessOrEqual(s.T(), expirySecs, opts.expiryBounds.MaxSecs)
	default:
		t.Fatalf("invalid test check, unknown expiry check type")
	}
}

func (s *GatewayOpsTestSuite) TestHelpers() {
	// we do a quick test of the basic Upsert into Get functionality first because
	// most of the tests below require that this basic functionality is working as
	// expected...
	testDocId := s.randomDocId()

	s.createDocument(createDocumentOptions{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		DocId:          testDocId,
		Content:        TEST_CONTENT,
		ContentFlags:   TEST_CONTENT_FLAGS,
	})

	s.checkDocument(s.T(), checkDocumentOptions{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		DocId:          testDocId,
		Content:        TEST_CONTENT,
		ContentFlags:   TEST_CONTENT_FLAGS,
	})
}

func (s *GatewayOpsTestSuite) CreateScope(bucket, scope string) func() {
	collectionAdminClient := admin_collection_v1.NewCollectionAdminServiceClient(s.gatewayConn)

	resp, err := collectionAdminClient.CreateScope(context.Background(), &admin_collection_v1.CreateScopeRequest{
		BucketName: bucket,
		ScopeName:  scope,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	return func() {
		delResp, err := collectionAdminClient.DeleteScope(context.Background(), &admin_collection_v1.DeleteScopeRequest{
			BucketName: bucket,
			ScopeName:  scope,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), delResp, err)
	}
}

func (s *GatewayOpsTestSuite) CreateCollection(bucket, scope, collection string) func() {
	collectionAdminClient := admin_collection_v1.NewCollectionAdminServiceClient(s.gatewayConn)

	resp, err := collectionAdminClient.CreateCollection(context.Background(), &admin_collection_v1.CreateCollectionRequest{
		BucketName:     bucket,
		ScopeName:      scope,
		CollectionName: collection,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	requireRpcSuccess(s.T(), resp, err)

	return func() {
		delResp, err := collectionAdminClient.DeleteCollection(context.Background(), &admin_collection_v1.DeleteCollectionRequest{
			BucketName:     bucket,
			ScopeName:      scope,
			CollectionName: collection,
		}, grpc.PerRPCCredentials(s.basicRpcCreds))
		requireRpcSuccess(s.T(), delResp, err)
	}
}

func (s *GatewayOpsTestSuite) getServerVersion() string {
	testConfig := testutils.GetTestConfig(s.T())

	mgmt := &cbmgmtx.Management{
		Transport: http.DefaultTransport,
		UserAgent: "useragent",
		Endpoint:  "http://" + testConfig.CbConnStr + ":8091",
		Username:  testConfig.CbUser,
		Password:  testConfig.CbPass,
	}

	clusterInfo, err := mgmt.GetClusterInfo(context.Background(), &cbmgmtx.GetClusterConfigOptions{})
	require.NoError(s.T(), err)

	// strip the meta-info like -enterprise or build numbers
	serverVersion := strings.Split(clusterInfo.ImplementationVersion, "-")[0]

	return serverVersion
}

func (s *GatewayOpsTestSuite) IsOlderServerVersion(checkVersion string) bool {
	serverVersion := s.getServerVersion()
	return semver.Compare("v"+serverVersion, "v"+checkVersion) < 0
}
