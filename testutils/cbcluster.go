package testutils

import (
	"context"
	"errors"
	"fmt"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbaselabs/gocbconnstr/v2"
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
	BucketName           string
	SecondBucketName     string
	ScopeName            string
	SecondScopeName      string
	CollectionName       string
	SecondCollectionName string
	AdminClient          *gocbcorex.Agent
}

type CanonicalTestClusterOptions struct {
	ConnStr  string
	Username string
	Password string
}

// SetupCanonicalTestCluster sets up a canonical test cluster for use throughout.
func SetupCanonicalTestCluster(opts CanonicalTestClusterOptions) (*CanonicalTestCluster, error) {
	baseSpec, err := gocbconnstr.Parse(opts.ConnStr)
	if err != nil {
		return nil, err
	}

	spec, err := gocbconnstr.Resolve(baseSpec)
	if err != nil {
		return nil, err
	}

	var httpHosts []string
	for _, specHost := range spec.HttpHosts {
		httpHosts = append(httpHosts, fmt.Sprintf("%s:%d", specHost.Host, specHost.Port))
	}

	var memdHosts []string
	for _, specHost := range spec.MemdHosts {
		memdHosts = append(memdHosts, fmt.Sprintf("%s:%d", specHost.Host, specHost.Port))
	}

	agent, err := gocbcorex.CreateAgent(context.Background(), gocbcorex.AgentOptions{
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: opts.Username,
			Password: opts.Password,
		},
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: httpHosts,
			MemdAddrs: memdHosts,
		},
	})
	if err != nil {
		return nil, err
	}

	defaultBucket, err := agent.GetBucket(context.Background(), &cbmgmtx.GetBucketOptions{
		BucketName: "default",
	})
	if err != nil {
		return nil, errors.New("test cluster must have a `default` bucket")
	}

	if !defaultBucket.FlushEnabled {
		return nil, errors.New("`default` must have flush enabled")
	}

	/*
		secondaryBucket, err := cbClient.Buckets().GetBucket("secBucket", nil)
		if err != nil {
			return nil, errors.New("test cluster must have a `secBucket` bucket")
		}

		if !secondaryBucket.FlushEnabled {
			return nil, errors.New("`secBucket` must have flush enabled")
		}
	*/

	// TODO(brett19): Perform additional canonical test cluster validations here...

	return &CanonicalTestCluster{
		ConnStr:   opts.ConnStr,
		AdminUser: opts.Username,
		AdminPass: opts.Password,
		// We don't currently generate basic/read-only users, so we
		// need to re-use our own credentials here instead.
		/*
			BasicUser:            "basic-user",
			BasicPass:            "password1",
			ReadUser:             "read-user",
			ReadPass:             "password2",
		*/
		BasicUser:            opts.Username,
		BasicPass:            opts.Password,
		BucketName:           "default",
		SecondBucketName:     "",
		ScopeName:            "_default",
		SecondScopeName:      "test-scope",
		CollectionName:       "_default",
		SecondCollectionName: "test-collection",
		AdminClient:          agent,
	}, nil
}

func (c *CanonicalTestCluster) Close() error {
	err := c.AdminClient.Close()
	if err != nil {
		return err
	}
	return nil
}
