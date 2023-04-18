package test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/stellar-gateway/contrib/grpcheaderauth"
	"github.com/couchbase/stellar-gateway/gateway"
	"github.com/couchbase/stellar-gateway/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type GatewayOpsTestSuite struct {
	suite.Suite

	testClusterInfo  *testutils.CanonicalTestCluster
	gatewayCloseFunc func()
	gatewayConn      *grpc.ClientConn
	gatewayClosedCh  chan struct{}

	bucketName     string
	scopeName      string
	collectionName string
	badRpcCreds    credentials.PerRPCCredentials
	basicRpcCreds  credentials.PerRPCCredentials
	readRpcCreds   credentials.PerRPCCredentials
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

func (s *GatewayOpsTestSuite) tooDeepJsonPath() string {
	var longPathParts []string
	for i := 0; i < 48; i++ {
		longPathParts = append(longPathParts, fmt.Sprintf("%c", 'a'+(i%26)))
	}
	return strings.Join(longPathParts, ".")
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
	assertRpcSuccess(s.T(), galResp, err)
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

func (s *GatewayOpsTestSuite) loadTestData(path string) []byte {
	b, err := os.ReadFile(path)
	s.NoErrorf(err, "Failed to read test data for %s", path)

	return b
}

func (s *GatewayOpsTestSuite) SetupSuite() {
	s.T().Logf("setting up gateway ops suite")

	testConfig := testutils.GetTestConfig(s.T())

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

	s.T().Logf("launching test gateway...")

	logger, err := zap.NewDevelopment()
	if err != nil {
		s.T().Fatalf("failed to initialize test logging: %s", err)
	}

	gwStartInfoCh := make(chan *gateway.StartupInfo, 1)

	gwCtx, gwCtxCancel := context.WithCancel(context.Background())
	gwClosedCh := make(chan struct{})
	go func() {
		err = gateway.Run(gwCtx, &gateway.Config{
			Logger:       logger.Named("gateway"),
			CbConnStr:    testConfig.CbConnStr,
			Username:     testConfig.CbUser,
			Password:     testConfig.CbPass,
			BindDataPort: 0,
			BindSdPort:   0,
			NumInstances: 1,

			StartupCallback: func(m *gateway.StartupInfo) {
				gwStartInfoCh <- m
			},
		})

		s.T().Logf("test gateway has shut down")
		close(gwClosedCh)
	}()

	startInfo := <-gwStartInfoCh

	connAddr := fmt.Sprintf("%s:%d", "127.0.0.1", startInfo.AdvertisePorts.PS)
	conn, err := grpc.Dial(connAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.T().Fatalf("failed to connect to test gateway: %s", err)
	}

	s.gatewayConn = conn
	s.gatewayCloseFunc = gwCtxCancel
	s.gatewayClosedCh = gwClosedCh
}

func (s *GatewayOpsTestSuite) TearDownSuite() {
	s.T().Logf("tearing down gateway ops suite")

	s.gatewayCloseFunc()
	<-s.gatewayClosedCh
	s.gatewayConn = nil
}

var TEST_CONTENT = []byte(`{"foo": "bar"}`)
var TEST_CONTENT_FLAGS = uint32(0x01000000)

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
		Content:        opts.Content,
		ContentFlags:   opts.ContentFlags,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	assertRpcSuccess(s.T(), upsertResp, err)
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
)

type checkDocumentOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	DocId          string
	Content        []byte
	ContentFlags   uint32

	expiry expiryCheckType
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
	assertRpcSuccess(s.T(), getResp, err)
	assertValidCas(s.T(), getResp.Cas)

	assert.Equal(s.T(), getResp.Content, opts.Content)
	assert.Equal(s.T(), getResp.ContentFlags, opts.ContentFlags)

	switch opts.expiry {
	case expiryCheckType_None:
		assert.Nil(s.T(), getResp.Expiry)
	case expiryCheckType_Set:
		assertValidTimestamp(s.T(), getResp.Expiry)
	case expiryCheckType_Future:
		assertValidTimestamp(s.T(), getResp.Expiry)
		ts := getResp.Expiry.AsTime()
		assert.True(t, !ts.Before(time.Now()))
	case expiryCheckType_Past:
		assertValidTimestamp(s.T(), getResp.Expiry)
		ts := getResp.Expiry.AsTime()
		assert.True(t, !ts.After(time.Now()))
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
