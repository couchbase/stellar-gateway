package server_v1

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/status"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbqueryx"
	"github.com/couchbase/goprotostellar/genproto/admin_query_v1"
)

type QueryIndexAdminServer struct {
	admin_query_v1.UnimplementedQueryAdminServiceServer

	logger       *zap.Logger
	errorHandler *ErrorHandler
	authHandler  *AuthHandler
}

func NewQueryIndexAdminServer(
	logger *zap.Logger,
	errorHandler *ErrorHandler,
	authHandler *AuthHandler,
) *QueryIndexAdminServer {
	return &QueryIndexAdminServer{
		logger:       logger,
		errorHandler: errorHandler,
		authHandler:  authHandler,
	}
}

func (s *QueryIndexAdminServer) buildKeyspace(
	bucket string,
	scope, collection *string,
) string {
	if scope != nil && collection != nil {
		return fmt.Sprintf("%s.%s.%s",
			cbqueryx.EncodeIdentifier(bucket),
			cbqueryx.EncodeIdentifier(*scope),
			cbqueryx.EncodeIdentifier(*collection))
	} else if collection == nil && scope != nil {
		return fmt.Sprintf("%s.%s.%s",
			cbqueryx.EncodeIdentifier(bucket),
			cbqueryx.EncodeIdentifier(*scope),
			cbqueryx.EncodeIdentifier("_default"))
	} else if collection != nil && scope == nil {
		return fmt.Sprintf("%s.%s.%s",
			cbqueryx.EncodeIdentifier(bucket),
			cbqueryx.EncodeIdentifier("_default"),
			cbqueryx.EncodeIdentifier(*collection))
	}

	return cbqueryx.EncodeIdentifier(bucket)
}

func (s *QueryIndexAdminServer) translateError(
	err error,
) *status.Status {
	return s.errorHandler.NewGenericStatus(err)
}

func (s *QueryIndexAdminServer) executeQuery(
	ctx context.Context,
	bucketName *string,
	statement string,
) ([]json.RawMessage, *status.Status) {
	agent, oboInfo, errSt := s.authHandler.GetHttpOboAgent(ctx, bucketName)
	if errSt != nil {
		return nil, errSt
	}

	var opts gocbcorex.QueryOptions
	opts.OnBehalfOf = oboInfo
	opts.Statement = statement
	result, err := agent.Query(ctx, &opts)
	if err != nil {
		return nil, s.translateError(err)
	}

	var rows []json.RawMessage
	for result.HasMoreRows() {
		rowBytes, err := result.ReadRow()
		if err != nil {
			return nil, s.translateError(err)
		}

		rows = append(rows, rowBytes)
	}

	return rows, nil
}

type queryIndexRowJson struct {
	Name        string   `json:"name"`
	IsPrimary   bool     `json:"is_primary"`
	Using       string   `json:"using"`
	State       string   `json:"state"`
	KeyspaceId  string   `json:"keyspace_id"`
	NamespaceId string   `json:"namespace_id"`
	IndexKey    []string `json:"index_key"`
	Condition   string   `json:"condition"`
	Partition   string   `json:"partition"`
	ScopeId     string   `json:"scope_id"`
	BucketId    string   `json:"bucket_id"`
}

func (s *QueryIndexAdminServer) GetAllIndexes(
	ctx context.Context,
	in *admin_query_v1.GetAllIndexesRequest,
) (*admin_query_v1.GetAllIndexesResponse, error) {
	var where string
	if in.CollectionName == nil && in.ScopeName == nil {
		if in.BucketName != nil {
			encodedBucket, _ := cbqueryx.EncodeValue(in.BucketName)
			where = fmt.Sprintf("(keyspace_id=%s AND bucket_id IS MISSING) OR bucket_id=%s", encodedBucket, encodedBucket)
		} else {
			where = "1=1"
		}
	} else {
		scopeName := "_default"
		if in.ScopeName != nil {
			scopeName = *in.ScopeName
		}

		collectionName := "_default"
		if in.CollectionName != nil {
			collectionName = *in.CollectionName
		}

		encodedBucket, _ := cbqueryx.EncodeValue(in.BucketName)
		encodedScope, _ := cbqueryx.EncodeValue(scopeName)
		encodedCollection, _ := cbqueryx.EncodeValue(collectionName)

		where = fmt.Sprintf("bucket_id=%s AND scope_id=%s AND keyspace_id=%s",
			encodedBucket, encodedScope, encodedCollection)

		if scopeName == "_default" && collectionName == "_default" {
			// When the user is querying for the default collection, we need to capture the index
			// case where there is only a keyspace_id, which implies the index is on the buckets default
			where = fmt.Sprintf("(%s) OR (keyspace_id=%s AND bucket_id IS MISSING)", where, encodedBucket)
		}
	}

	where = fmt.Sprintf("(%s) AND `using`=\"gsi\"", where)

	qs := fmt.Sprintf("SELECT `idx`.* FROM system:indexes AS idx WHERE %s ORDER BY is_primary DESC, name ASC",
		where)

	rows, errSt := s.executeQuery(ctx, in.BucketName, qs)
	if errSt != nil {
		return nil, errSt.Err()
	}

	var indexes []*admin_query_v1.GetAllIndexesResponse_Index

	for _, rowBytes := range rows {
		var row queryIndexRowJson
		err := json.Unmarshal(rowBytes, &row)
		if err != nil {
			return nil, s.errorHandler.NewGenericStatus(err).Err()
		}

		state, errSt := indexStateFromQueryTableString(row.State)
		if errSt != nil {
			return nil, errSt.Err()
		}

		index := &admin_query_v1.GetAllIndexesResponse_Index{
			Name:      row.Name,
			IsPrimary: row.IsPrimary,
			Type:      admin_query_v1.IndexType_INDEX_TYPE_GSI,
			State:     state,
			Fields:    row.IndexKey,
		}

		if row.BucketId == "" {
			defaultScopeColl := "_default"

			index.BucketName = row.KeyspaceId
			index.ScopeName = defaultScopeColl
			index.CollectionName = defaultScopeColl
		} else {
			index.BucketName = row.BucketId
			index.ScopeName = row.ScopeId
			index.CollectionName = row.KeyspaceId
		}

		if row.Condition != "" {
			index.Condition = &row.Condition
		}

		if row.Partition != "" {
			index.Partition = &row.Partition
		}

		indexes = append(indexes, index)
	}

	return &admin_query_v1.GetAllIndexesResponse{
		Indexes: indexes,
	}, nil
}

func (s *QueryIndexAdminServer) CreatePrimaryIndex(
	ctx context.Context,
	in *admin_query_v1.CreatePrimaryIndexRequest,
) (*admin_query_v1.CreatePrimaryIndexResponse, error) {
	var qs string

	qs += "CREATE PRIMARY INDEX"

	if in.Name != nil {
		qs += " " + cbqueryx.EncodeIdentifier(*in.Name)
	}

	qs += " ON " + s.buildKeyspace(in.BucketName, in.ScopeName, in.CollectionName)

	with := make(map[string]interface{})

	if in.Deferred != nil {
		with["defer_build"] = *in.Deferred
	}

	if in.NumReplicas != nil {
		with["num_replica"] = *in.NumReplicas
	}

	if len(with) > 0 {
		withBytes, err := json.Marshal(with)
		if err != nil {
			return nil, s.errorHandler.NewGenericStatus(err).Err()
		}

		qs += " WITH " + string(withBytes)
	}

	_, errSt := s.executeQuery(ctx, &in.BucketName, qs)
	if errSt != nil {
		return nil, errSt.Err()
	}

	return &admin_query_v1.CreatePrimaryIndexResponse{}, nil
}

func (s *QueryIndexAdminServer) CreateIndex(
	ctx context.Context,
	in *admin_query_v1.CreateIndexRequest,
) (*admin_query_v1.CreateIndexResponse, error) {
	var qs string

	qs += "CREATE INDEX"

	qs += " " + cbqueryx.EncodeIdentifier(in.Name)

	qs += " ON " + s.buildKeyspace(in.BucketName, in.ScopeName, in.CollectionName)

	if len(in.Fields) == 0 {
		return nil, s.errorHandler.NewNeedIndexFieldsStatus().Err()
	}

	encodedFields := make([]string, len(in.Fields))
	for fieldIdx, field := range in.Fields {
		encodedFields[fieldIdx] = cbqueryx.EncodeIdentifier(field)
	}
	qs += " (" + strings.Join(encodedFields, ",") + ")"

	with := make(map[string]interface{})

	if in.Deferred != nil {
		with["defer_build"] = *in.Deferred
	}

	if in.NumReplicas != nil {
		with["num_replica"] = *in.NumReplicas
	}

	if len(with) > 0 {
		withBytes, err := json.Marshal(with)
		if err != nil {
			return nil, s.errorHandler.NewGenericStatus(err).Err()
		}

		qs += " WITH " + string(withBytes)
	}

	_, errSt := s.executeQuery(ctx, &in.BucketName, qs)
	if errSt != nil {
		return nil, errSt.Err()
	}

	return &admin_query_v1.CreateIndexResponse{}, nil
}

func (s *QueryIndexAdminServer) DropPrimaryIndex(
	ctx context.Context,
	in *admin_query_v1.DropPrimaryIndexRequest,
) (*admin_query_v1.DropPrimaryIndexResponse, error) {
	var qs string

	keyspace := s.buildKeyspace(in.BucketName, in.ScopeName, in.CollectionName)

	if in.Name == nil {
		qs += fmt.Sprintf("DROP PRIMARY INDEX ON %s", keyspace)
	} else {
		encodedName := cbqueryx.EncodeIdentifier(*in.Name)

		if in.ScopeName != nil || in.CollectionName != nil {
			qs += fmt.Sprintf("DROP INDEX %s ON %s", encodedName, keyspace)
		} else {
			qs += fmt.Sprintf("DROP INDEX %s.%s", keyspace, encodedName)
		}
	}

	_, errSt := s.executeQuery(ctx, &in.BucketName, qs)
	if errSt != nil {
		return nil, errSt.Err()
	}

	return &admin_query_v1.DropPrimaryIndexResponse{}, nil
}

func (s *QueryIndexAdminServer) DropIndex(
	ctx context.Context,
	in *admin_query_v1.DropIndexRequest,
) (*admin_query_v1.DropIndexResponse, error) {
	var qs string

	encodedName := cbqueryx.EncodeIdentifier(in.Name)
	keyspace := s.buildKeyspace(in.BucketName, in.ScopeName, in.CollectionName)

	if in.ScopeName != nil || in.CollectionName != nil {
		qs += fmt.Sprintf("DROP INDEX %s ON %s", encodedName, keyspace)
	} else {
		qs += fmt.Sprintf("DROP INDEX %s.%s", keyspace, encodedName)
	}

	_, errSt := s.executeQuery(ctx, &in.BucketName, qs)
	if errSt != nil {
		return nil, errSt.Err()
	}

	return &admin_query_v1.DropIndexResponse{}, nil
}

func (s *QueryIndexAdminServer) BuildDeferredIndexes(
	ctx context.Context,
	in *admin_query_v1.BuildDeferredIndexesRequest,
) (*admin_query_v1.BuildDeferredIndexesResponse, error) {
	getIndexesResp, err := s.GetAllIndexes(ctx, &admin_query_v1.GetAllIndexesRequest{
		BucketName:     &in.BucketName,
		ScopeName:      in.ScopeName,
		CollectionName: in.CollectionName,
	})
	if err != nil {
		return nil, err
	}

	var deferredIndexNames []string
	for _, index := range getIndexesResp.Indexes {
		if index.State == admin_query_v1.IndexState_INDEX_STATE_DEFERRED {
			deferredIndexNames = append(deferredIndexNames, index.Name)
		}
	}

	if len(deferredIndexNames) == 0 {
		// If there are no indexes left to build, we can just return success
		return &admin_query_v1.BuildDeferredIndexesResponse{}, nil
	}

	keyspace := s.buildKeyspace(in.BucketName, in.ScopeName, in.CollectionName)

	escapedIndexNames := make([]string, len(deferredIndexNames))
	for indexIdx, indexName := range deferredIndexNames {
		escapedIndexNames[indexIdx] = cbqueryx.EncodeIdentifier(indexName)
	}

	var qs string
	qs += fmt.Sprintf("BUILD INDEX ON %s(%s)",
		keyspace, strings.Join(escapedIndexNames, ","))

	_, errSt := s.executeQuery(ctx, &in.BucketName, qs)
	if errSt != nil {
		return nil, errSt.Err()
	}

	for {
		watchIndexesResp, err := s.GetAllIndexes(ctx, &admin_query_v1.GetAllIndexesRequest{
			BucketName:     &in.BucketName,
			ScopeName:      in.ScopeName,
			CollectionName: in.CollectionName,
		})
		if err != nil {
			return nil, err
		}

		getIndex := func(indexName string) *admin_query_v1.GetAllIndexesResponse_Index {
			for _, index := range watchIndexesResp.Indexes {
				if index.Name == indexName {
					return index
				}
			}
			return nil
		}

		allIndexesBuilding := true
		for _, indexName := range deferredIndexNames {
			index := getIndex(indexName)
			if index == nil {
				// if the index is not found at all, just consider it building
				s.logger.Warn("an index that was scheduled for building is no longer found", zap.String("indexName", indexName))
				continue
			}

			if index.State == admin_query_v1.IndexState_INDEX_STATE_DEFERRED {
				allIndexesBuilding = false
				break
			}
		}

		// if some of the indexes still haven't transitioned out of the deferred state,
		// we wait 100ms and then scan to see if the index has transitioned.
		if !allIndexesBuilding {
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			continue
		}

		break
	}

	return &admin_query_v1.BuildDeferredIndexesResponse{}, nil
}
