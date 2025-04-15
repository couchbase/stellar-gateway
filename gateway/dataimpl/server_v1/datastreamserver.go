package server_v1

import (
	"log"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbase/goprotostellar/genproto/datastream_v1"
	"github.com/couchbaselabs/gojsonsm"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DatastreamServer struct {
	datastream_v1.UnimplementedDatastreamServiceServer

	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
}

func NewDatastreamServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
) *DatastreamServer {
	return &DatastreamServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
	}
}

func (s *DatastreamServer) StreamCollection(
	in *datastream_v1.StreamCollectionRequest,
	out datastream_v1.DatastreamService_StreamCollectionServer,
) error {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(out.Context(), &in.BucketName)
	if errSt != nil {
		return errSt.Err()
	}

	var matcher gojsonsm.Matcher = nil
	if in.Filter != nil && *in.Filter != "" {
		parsedMatcher, err := gojsonsm.GetFilterExpressionMatcher(*in.Filter)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to parse filter expression: %v", err)
		}

		matcher = parsedMatcher
	}

	mgmtEps, err := agent.GetMgmtEndpoints()
	if err != nil {
		return err
	}

	auth := &gocbcorex.PasswordAuthenticator{
		Username: oboInfo.Username,
		Password: oboInfo.Password,
	}

	closeCh := make(chan struct{}, 1)

	dcpAgentUuid := uuid.NewString()[:6]
	dcpAgentName := "dcp-agent-" + dcpAgentUuid
	dcpAgent, err := gocbcorex.CreateDcpAgent(out.Context(), gocbcorex.DcpAgentOptions{
		Logger:        s.logger.Named(dcpAgentName),
		TLSConfig:     nil,
		Authenticator: auth,
		BucketName:    in.BucketName,
		SeedMgmtAddrs: mgmtEps,
		StreamOptions: gocbcorex.DcpStreamOptions{
			ConnectionName: dcpAgentName,
			NoopInterval:   5 * time.Second,
		},
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create DCP agent: %v", err)
	}

	streamSet, err := dcpAgent.NewStreamSet(&gocbcorex.NewStreamSetOptions{
		Handlers: gocbcorex.DcpEventsHandlers{
			Mutation: func(evt *memdx.DcpMutationEvent) {
				calculatedVbId := dcpAgent.KeyToVbucket(evt.Key)
				if calculatedVbId != evt.VbucketId {
					// skipping because it's a key outside it's 'canonical' vbucket
					return
				}

				if matcher != nil {
					matcher.Reset()
					matches, err := matcher.Match(evt.Value)
					if err != nil {
						s.logger.Debug("matcher failed to execute", zap.Error(err))
						return
					}

					if !matches {
						// skipping document that does not match
						return
					}
				}

				out.Send(&datastream_v1.StreamCollectionResponse{
					Response: &datastream_v1.StreamCollectionResponse_Mutation{
						Mutation: &datastream_v1.StreamCollectionMutation{
							Key:   string(evt.Key),
							Value: evt.Value,
						},
					},
				})
			},
		},
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create dcp stream set: %v", err)
	}

	numVbs := dcpAgent.NumVbuckets()

	collectionId, _, err := dcpAgent.GetCollectionId(out.Context(), in.ScopeName, in.CollectionName)
	if err != nil {
		log.Printf("Failed to get collectionId: %v", err)
		return err
	}

	vbOpenCh := make(chan error, numVbs)
	for vbId := uint16(0); vbId < numVbs; vbId++ {
		go func() {
			highSeqNo, vbUuid, err := dcpAgent.GetVbucketHighSeqNo(out.Context(), vbId)
			if err != nil {
				log.Printf("Failed to get vbucket highseqno: %v", err)
				vbOpenCh <- err
				return
			}

			_, err = streamSet.OpenVbucket(out.Context(), &gocbcorex.OpenVbucketOptions{
				VbucketId:      vbId,
				Flags:          0,
				StartSeqNo:     highSeqNo,
				EndSeqNo:       0xffffffffffffffff,
				VbUuid:         vbUuid,
				SnapStartSeqNo: highSeqNo,
				SnapEndSeqNo:   0xffffffffffffffff,

				CollectionIds: []uint32{collectionId},
			})
			if err != nil {
				log.Printf("Failed to start vbucket stream: %v", err)
				vbOpenCh <- err
			}

			vbOpenCh <- nil
		}()
	}

	for vbId := uint16(0); vbId < numVbs; vbId++ {
		err := <-vbOpenCh
		if err != nil {
			return err
		}
	}

	<-closeCh
	return nil
}
