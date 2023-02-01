package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/stellar-nebula/contrib/grpcheaderauth"
	"github.com/couchbase/stellar-nebula/gateway"
	"github.com/couchbase/stellar-nebula/testutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
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
	basicRpcCreds  credentials.PerRPCCredentials
	readRpcCreds   credentials.PerRPCCredentials
}

func (s *GatewayOpsTestSuite) randomDocId(prefix string) string {
	return prefix + "_" + uuid.NewString()
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

type createDocumentOptions struct {
	BucketName     string
	ScopeName      string
	CollectionName string
	DocId          string
	Content        []byte
}

func (s *GatewayOpsTestSuite) createDocument(t *testing.T, opts createDocumentOptions) {
	kvClient := kv_v1.NewKvClient(s.gatewayConn)

	upsertResp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
		BucketName:     opts.BucketName,
		ScopeName:      opts.ScopeName,
		CollectionName: opts.CollectionName,
		Key:            opts.DocId,
		ContentType:    kv_v1.DocumentContentType_JSON,
		Content:        TEST_CONTENT,
	})
	assertRpcSuccess(s.T(), upsertResp, err)
	assertValidCas(s.T(), upsertResp.Cas)
	assertValidMutationToken(s.T(), upsertResp.MutationToken, s.bucketName)
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

	contentType     *kv_v1.DocumentContentType
	compressionType *kv_v1.DocumentCompressionType
	expiry          expiryCheckType
}

func (o checkDocumentOptions) ApplyMissingDefaults() checkDocumentOptions {
	if o.contentType == nil {
		defaultContentType := kv_v1.DocumentContentType_JSON
		o.contentType = &defaultContentType
	}
	if o.compressionType == nil {
		defaultCompressionType := kv_v1.DocumentCompressionType_NONE
		o.compressionType = &defaultCompressionType
	}
	return o
}

func (o checkDocumentOptions) ExpectContentType(contentType kv_v1.DocumentContentType) checkDocumentOptions {
	o.contentType = &contentType
	return o
}

func (o checkDocumentOptions) ExpectCompressionType(compressionType kv_v1.DocumentCompressionType) checkDocumentOptions {
	o.compressionType = &compressionType
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

	kvClient := kv_v1.NewKvClient(s.gatewayConn)

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
	assert.Equal(s.T(), getResp.ContentType, *opts.contentType)
	assert.Equal(s.T(), getResp.CompressionType, *opts.compressionType)
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
	testDocId := s.randomDocId("helper-check")

	s.createDocument(s.T(), createDocumentOptions{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		DocId:          testDocId,
		Content:        TEST_CONTENT,
	})

	s.checkDocument(s.T(), checkDocumentOptions{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		DocId:          testDocId,
		Content:        TEST_CONTENT,
	})
}

func (s *GatewayOpsTestSuite) TestGet() {
	kvClient := kv_v1.NewKvClient(s.gatewayConn)

	testDocId := s.randomDocId("basic-crud")

	s.createDocument(s.T(), createDocumentOptions{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		DocId:          testDocId,
		Content:        TEST_CONTENT,
	})

	getResp, err := kvClient.Get(context.Background(), &kv_v1.GetRequest{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		Key:            testDocId,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	assertRpcSuccess(s.T(), getResp, err)
	assertValidCas(s.T(), getResp.Cas)
	assert.Equal(s.T(), getResp.CompressionType, kv_v1.DocumentCompressionType_NONE)
	assert.Equal(s.T(), getResp.Content, TEST_CONTENT)
	assert.Equal(s.T(), getResp.ContentType, kv_v1.DocumentContentType_JSON)
	assert.Nil(s.T(), getResp.Expiry)

	// Test doing a Get where the document is missing
	missingDocId := s.randomDocId("missing-doc")
	_, err = kvClient.Get(context.Background(), &kv_v1.GetRequest{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		Key:            missingDocId,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	assertRpcStatus(s.T(), err, codes.NotFound)
	assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
		assert.Equal(s.T(), d.ResourceType, "document")
	})
}

func (s *GatewayOpsTestSuite) TestInsert() {
	kvClient := kv_v1.NewKvClient(s.gatewayConn)

	testDocId := s.randomDocId("insert-op")

	// Test doing a normal insert
	insertResp, err := kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		Key:            testDocId,
		ContentType:    kv_v1.DocumentContentType_JSON,
		Content:        TEST_CONTENT,
	})
	assertRpcStatus(s.T(), err, codes.OK)
	assertValidCas(s.T(), insertResp.Cas)
	assertValidMutationToken(s.T(), insertResp.MutationToken, s.bucketName)

	s.checkDocument(s.T(), checkDocumentOptions{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		DocId:          testDocId,
		Content:        TEST_CONTENT,
	})

	// Test doing an insert where the document already exists
	_, err = kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		Key:            testDocId,
		ContentType:    kv_v1.DocumentContentType_JSON,
		Content:        TEST_CONTENT,
	})
	assertRpcStatus(s.T(), err, codes.AlreadyExists)
	assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
		assert.Equal(s.T(), d.ResourceType, "document")
	})

	/*
		// Test doing an insert to an invalid bucket
		_, err = kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     "invalid-bucket",
			ScopeName:      s.scopeName,
			CollectionName: s.collectionName,
			Key:            testDocId,
			ContentType:    kv_v1.DocumentContentType_JSON,
			Content:        testContent,
		})
		assertRpcStatus(s.T(), err, codes.AlreadyExists)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "bucket")
		})

		// Test doing an insert to an invalid scope
		_, err = kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      "invalid-scope",
			CollectionName: s.collectionName,
			Key:            testDocId,
			ContentType:    kv_v1.DocumentContentType_JSON,
			Content:        testContent,
		})
		assertRpcStatus(s.T(), err, codes.AlreadyExists)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "scope")
		})

		// Test doing an insert to an invalid collection
		_, err = kvClient.Insert(context.Background(), &kv_v1.InsertRequest{
			BucketName:     s.bucketName,
			ScopeName:      s.scopeName,
			CollectionName: "invalid-collection",
			Key:            testDocId,
			ContentType:    kv_v1.DocumentContentType_JSON,
			Content:        testContent,
		})
		assertRpcStatus(s.T(), err, codes.AlreadyExists)
		assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
			assert.Equal(s.T(), d.ResourceType, "collection")
		})
	*/
}

func (s *GatewayOpsTestSuite) TestUpsert() {
	kvClient := kv_v1.NewKvClient(s.gatewayConn)

	testDocId := s.randomDocId("upsert-op")

	upsertResp, err := kvClient.Upsert(context.Background(), &kv_v1.UpsertRequest{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		Key:            testDocId,
		ContentType:    kv_v1.DocumentContentType_JSON,
		Content:        TEST_CONTENT,
	})
	assertRpcSuccess(s.T(), upsertResp, err)
	assertValidCas(s.T(), upsertResp.Cas)
	assertValidMutationToken(s.T(), upsertResp.MutationToken, s.bucketName)

	s.checkDocument(s.T(), checkDocumentOptions{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		DocId:          testDocId,
		Content:        TEST_CONTENT,
	})
}

func (s *GatewayOpsTestSuite) TestReplace() {
	kvClient := kv_v1.NewKvClient(s.gatewayConn)

	testDocId := s.randomDocId("replace-op")
	testContent := []byte(`{"foo": "bar"}`)
	testContentRep := []byte(`{"boo": "baz"}`)

	s.createDocument(s.T(), createDocumentOptions{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		DocId:          testDocId,
		Content:        testContent,
	})

	// Test doing a replace without a cas
	repResp, err := kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		Key:            testDocId,
		Content:        testContentRep,
		ContentType:    kv_v1.DocumentContentType_JSON,
		Cas:            nil,
		Expiry:         nil,
		DurabilitySpec: nil,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	assertRpcStatus(s.T(), err, codes.OK)
	assertValidCas(s.T(), repResp.Cas)
	assertValidMutationToken(s.T(), repResp.MutationToken, s.bucketName)

	s.checkDocument(s.T(), checkDocumentOptions{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		DocId:          testDocId,
		Content:        testContentRep,
	})

	// Test doing a replace when the document is missing
	missingDocId := s.randomDocId("missing-doc")
	_, err = kvClient.Replace(context.Background(), &kv_v1.ReplaceRequest{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		Key:            missingDocId,
		Content:        TEST_CONTENT,
		ContentType:    kv_v1.DocumentContentType_JSON,
		Cas:            nil,
		Expiry:         nil,
		DurabilitySpec: nil,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	assertRpcStatus(s.T(), err, codes.NotFound)
	assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
		assert.Equal(s.T(), d.ResourceType, "document")
	})
}

func (s *GatewayOpsTestSuite) TestRemove() {
	kvClient := kv_v1.NewKvClient(s.gatewayConn)

	testDocId := s.randomDocId("remove-op")

	s.createDocument(s.T(), createDocumentOptions{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		DocId:          testDocId,
		Content:        TEST_CONTENT,
	})

	// Test doing a remove without a cas
	repResp, err := kvClient.Remove(context.Background(), &kv_v1.RemoveRequest{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		Key:            testDocId,
		Cas:            nil,
		DurabilitySpec: nil,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	assertRpcStatus(s.T(), err, codes.OK)
	assertValidCas(s.T(), repResp.Cas)
	assertValidMutationToken(s.T(), repResp.MutationToken, s.bucketName)

	s.checkDocument(s.T(), checkDocumentOptions{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		DocId:          testDocId,
		Content:        nil,
	})

	// Test doing a remove when the document is missing
	missingDocId := s.randomDocId("missing-doc")
	_, err = kvClient.Remove(context.Background(), &kv_v1.RemoveRequest{
		BucketName:     s.bucketName,
		ScopeName:      s.scopeName,
		CollectionName: s.collectionName,
		Key:            missingDocId,
		Cas:            nil,
		DurabilitySpec: nil,
	}, grpc.PerRPCCredentials(s.basicRpcCreds))
	assertRpcStatus(s.T(), err, codes.NotFound)
	assertRpcErrorDetails(s.T(), err, func(d *epb.ResourceInfo) {
		assert.Equal(s.T(), d.ResourceType, "document")
	})
}

func TestGatewayOps(t *testing.T) {
	suite.Run(t, new(GatewayOpsTestSuite))
}
