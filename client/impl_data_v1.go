package client

import (
	"context"

	data_v1 "github.com/couchbase/stellar-nebula/genproto/data/v1"
	"google.golang.org/grpc"
)

type routingImpl_DataV1 struct {
	client *RoutingClient
}

// Verify that RoutingClient implements Conn
var _ data_v1.DataClient = (*routingImpl_DataV1)(nil)

func (c *routingImpl_DataV1) Get(ctx context.Context, in *data_v1.GetRequest, opts ...grpc.CallOption) (*data_v1.GetResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().Get(ctx, in, opts...)
}

func (c *routingImpl_DataV1) GetAndTouch(ctx context.Context, in *data_v1.GetAndTouchRequest, opts ...grpc.CallOption) (*data_v1.GetResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().GetAndTouch(ctx, in, opts...)
}

func (c *routingImpl_DataV1) GetAndLock(ctx context.Context, in *data_v1.GetAndLockRequest, opts ...grpc.CallOption) (*data_v1.GetResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().GetAndLock(ctx, in, opts...)
}

func (c *routingImpl_DataV1) Unlock(ctx context.Context, in *data_v1.UnlockRequest, opts ...grpc.CallOption) (*data_v1.UnlockResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().Unlock(ctx, in, opts...)
}

func (c *routingImpl_DataV1) GetReplica(ctx context.Context, in *data_v1.GetReplicaRequest, opts ...grpc.CallOption) (*data_v1.GetResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().GetReplica(ctx, in, opts...)
}

func (c *routingImpl_DataV1) Touch(ctx context.Context, in *data_v1.TouchRequest, opts ...grpc.CallOption) (*data_v1.TouchResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().Touch(ctx, in, opts...)
}

func (c *routingImpl_DataV1) Exists(ctx context.Context, in *data_v1.ExistsRequest, opts ...grpc.CallOption) (*data_v1.ExistsResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().Exists(ctx, in, opts...)
}

func (c *routingImpl_DataV1) Insert(ctx context.Context, in *data_v1.InsertRequest, opts ...grpc.CallOption) (*data_v1.InsertResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().Insert(ctx, in, opts...)
}

func (c *routingImpl_DataV1) Upsert(ctx context.Context, in *data_v1.UpsertRequest, opts ...grpc.CallOption) (*data_v1.UpsertResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().Upsert(ctx, in, opts...)
}

func (c *routingImpl_DataV1) Replace(ctx context.Context, in *data_v1.ReplaceRequest, opts ...grpc.CallOption) (*data_v1.ReplaceResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().Replace(ctx, in, opts...)
}

func (c *routingImpl_DataV1) Remove(ctx context.Context, in *data_v1.RemoveRequest, opts ...grpc.CallOption) (*data_v1.RemoveResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().Remove(ctx, in, opts...)
}

func (c *routingImpl_DataV1) Increment(ctx context.Context, in *data_v1.IncrementRequest, opts ...grpc.CallOption) (*data_v1.IncrementResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().Increment(ctx, in, opts...)
}

func (c *routingImpl_DataV1) Decrement(ctx context.Context, in *data_v1.DecrementRequest, opts ...grpc.CallOption) (*data_v1.DecrementResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().Decrement(ctx, in, opts...)
}

func (c *routingImpl_DataV1) Append(ctx context.Context, in *data_v1.AppendRequest, opts ...grpc.CallOption) (*data_v1.AppendResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().Append(ctx, in, opts...)
}

func (c *routingImpl_DataV1) Prepend(ctx context.Context, in *data_v1.PrependRequest, opts ...grpc.CallOption) (*data_v1.PrependResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().Prepend(ctx, in, opts...)
}

func (c *routingImpl_DataV1) LookupIn(ctx context.Context, in *data_v1.LookupInRequest, opts ...grpc.CallOption) (*data_v1.LookupInResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().LookupIn(ctx, in, opts...)
}

func (c *routingImpl_DataV1) MutateIn(ctx context.Context, in *data_v1.MutateInRequest, opts ...grpc.CallOption) (*data_v1.MutateInResponse, error) {
	return c.client.fetchConnForKey(in.BucketName, in.Key).DataV1().MutateIn(ctx, in, opts...)
}

func (c *routingImpl_DataV1) RangeScan(ctx context.Context, in *data_v1.RangeScanRequest, opts ...grpc.CallOption) (*data_v1.RangeScanResponse, error) {
	return c.client.fetchConnForBucket(in.BucketName).DataV1().RangeScan(ctx, in, opts...)
}
