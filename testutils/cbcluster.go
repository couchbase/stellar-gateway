package testutils

import (
	"errors"

	"github.com/couchbase/gocb/v2"
)

/*
CanonicalTestCluster represents a properly configured canonical test cluster.  This
has 3 users with varying permissions along with various buckets/scopes/collections.
*/
type CanonicalTestCluster struct {
	ConnStr              string
	AdminUser            string
	AdminPass            string
	BasicUser            string
	BasicPass            string
	ReadUser             string
	ReadPass             string
	BucketName           string
	SecondBucketName     string
	ScopeName            string
	SecondScopeName      string
	CollectionName       string
	SecondCollectionName string
	AdminClient          *gocb.Cluster
}

type CanonicalTestClusterOptions struct {
	ConnStr  string
	Username string
	Password string
}

// SetupCanonicalTestCluster sets up a canonical test cluster for use throughout.
func SetupCanonicalTestCluster(opts CanonicalTestClusterOptions) (*CanonicalTestCluster, error) {
	cbClient, err := gocb.Connect(opts.ConnStr, gocb.ClusterOptions{
		Username: opts.Username,
		Password: opts.Password,
	})
	if err != nil {
		return nil, err
	}

	defaultBucket, err := cbClient.Buckets().GetBucket("default", nil)
	if err != nil {
		return nil, errors.New("test cluster must have a `default` bucket")
	}

	if !defaultBucket.FlushEnabled {
		return nil, errors.New("`default` must have flush enabled")
	}

	secondaryBucket, err := cbClient.Buckets().GetBucket("secBucket", nil)
	if err != nil {
		return nil, errors.New("test cluster must have a `secBucket` bucket")
	}

	if !secondaryBucket.FlushEnabled {
		return nil, errors.New("`secBucket` must have flush enabled")
	}

	// TODO(brett19): Perform additional canonical test cluster validations here...

	return &CanonicalTestCluster{
		ConnStr:              opts.ConnStr,
		AdminUser:            opts.Username,
		AdminPass:            opts.Password,
		BasicUser:            "basic-user",
		BasicPass:            "password1",
		ReadUser:             "read-user",
		ReadPass:             "password2",
		BucketName:           "default",
		SecondBucketName:     "secBucket",
		ScopeName:            "_default",
		SecondScopeName:      "test-scope",
		CollectionName:       "_default",
		SecondCollectionName: "test-collection",
		AdminClient:          cbClient,
	}, nil
}

func (c *CanonicalTestCluster) Close() error {
	c.AdminClient.Close(nil)
	return nil
}