/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package cbconfig

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"go.uber.org/zap"
)

// TODO(brett19): Somehow setup sharing of the `cbconfig` stuff.
// It seems to me that we use the config JSON code in all sorts of places, from gocbcore to
// gocaves and now here in stellar-nebula.  It would be ideal if we had some central place
// to keep these JSON definitions where we could reuse them...

// TODO(brett19): Need to add support for $HOST replacement, but this requires us to do a
// streaming replace on the IO stream, since we will support streaming configurations.

type FetcherOptions struct {
	HttpClient *http.Client
	Host       string
	Username   string
	Password   string
	Logger     *zap.Logger
}

type Fetcher struct {
	httpClient *http.Client
	host       string
	username   string
	password   string
	logger     *zap.Logger
}

func NewFetcher(opts FetcherOptions) *Fetcher {
	httpClient := opts.HttpClient
	if httpClient == nil {
		httpClient = &http.Client{}
	}

	return &Fetcher{
		httpClient: httpClient,
		host:       opts.Host,
		username:   opts.Username,
		password:   opts.Password,
		logger:     opts.Logger,
	}
}

// used to derive the hostname to use for $HOST replacement
func (f *Fetcher) deriveHostname() string {
	u, err := url.Parse(f.host)
	if err != nil {
		return f.host
	}

	return u.Hostname()
}

func (f *Fetcher) newRequest(ctx context.Context, method, path string) (*http.Request, error) {
	url := f.host + path

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}

	if f.username != "" || f.password != "" {
		req.SetBasicAuth(f.username, f.password)
	}

	return req, nil
}

func (f *Fetcher) doGetJson(ctx context.Context, path string, data any) error {
	req, err := f.newRequest(ctx, "GET", path)
	if err != nil {
		return err
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}

	// decode the response body
	decoder := json.NewDecoder(resp.Body)

	// decode into the config
	err = decoder.Decode(data)
	if err != nil {
		return err
	}

	// make sure the body is closed
	err = resp.Body.Close()
	if err != nil {
		f.logger.Error("unexpected close error", zap.Error(err))
	}

	return nil
}

func (f *Fetcher) doGetJsonConfig(ctx context.Context, path string, data any) error {
	// we use an intermediary so that we can replace $HOST
	var configBytes json.RawMessage
	err := f.doGetJson(ctx, path, &configBytes)
	if err != nil {
		return err
	}

	hostname := f.deriveHostname()
	configBytes = bytes.ReplaceAll(configBytes, []byte("$HOST"), []byte(hostname))

	err = json.Unmarshal(configBytes, data)
	if err != nil {
		return err
	}

	return nil
}

func (f *Fetcher) FetchNodeServices(ctx context.Context) (*TerseConfigJson, error) {
	var config TerseConfigJson
	err := f.doGetJsonConfig(ctx, "/pools/default/nodeServices", &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func (f *Fetcher) FetchServerGroups(ctx context.Context) (*ServerGroupConfigJson, error) {
	var config ServerGroupConfigJson
	err := f.doGetJsonConfig(ctx, "/pools/default/serverGroups", &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func (f *Fetcher) FetchTerseBucket(ctx context.Context, bucketName string) (*TerseConfigJson, error) {
	var config TerseConfigJson
	err := f.doGetJsonConfig(ctx, fmt.Sprintf("/pools/default/b/%s", bucketName), &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
