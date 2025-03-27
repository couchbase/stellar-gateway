/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package secretsmanager

import (
	"context"
	"fmt"
	"strings"

	gcpsecretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/keyvault/azsecrets"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

func FetchAWSSecret(secretId string, region string) (string, string, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		return "", "", fmt.Errorf("failed to load default aws config: %w", err)
	}

	secrets := secretsmanager.NewFromConfig(cfg)
	res, err := secrets.GetSecretValue(
		context.Background(),
		&secretsmanager.GetSecretValueInput{SecretId: &secretId},
	)
	if err != nil {
		return "", "", fmt.Errorf("failed to get aws secret: %w", err)
	}
	if res.SecretString == nil {
		return "", "", fmt.Errorf("aws secret %s not a string", secretId)
	}

	return credsFromSecret(*res.SecretString)
}

func FetchAzureSecret(secretId string, keyVaultName string) (string, string, error) {
	vaultURI := fmt.Sprintf("https://%s.vault.azure.net/", keyVaultName)

	// Create a credential using the NewDefaultAzureCredential type.
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return "", "", fmt.Errorf("failed to obtain azure credential: %w", err)
	}

	client, err := azsecrets.NewClient(vaultURI, cred, nil)
	if err != nil {
		return "", "", fmt.Errorf("failed to create azure client: %w", err)
	}

	//  An empty string version gets the latest version of the secret.
	version := ""
	resp, err := client.GetSecret(context.Background(), secretId, version, nil)
	if err != nil {
		return "", "", fmt.Errorf("failed to get azure secret: %w", err)
	}

	return credsFromSecret(*resp.Value)
}

func FetchGcpSecret(secretId string, projectId string) (string, string, error) {
	ctx := context.Background()
	client, err := gcpsecretmanager.NewClient(ctx)
	if err != nil {
		return "", "", fmt.Errorf("failed to create gcp secretmanager client: %w", err)
	}
	defer client.Close()

	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: fmt.Sprintf("projects/%s/secrets/%s/versions/latest", projectId, secretId),
	}

	result, err := client.AccessSecretVersion(ctx, req)
	if err != nil {
		return "", "", fmt.Errorf("failed to get gcp secret: %w", err)
	}

	return credsFromSecret(string(result.Payload.Data[:]))
}

func credsFromSecret(secret string) (string, string, error) {
	creds := strings.Split(secret, ":")
	if len(creds) != 2 {
		return "", "", fmt.Errorf("couchbase server credentials secret must be formatted `username:password`")
	}

	username := creds[0]
	password := creds[1]
	return username, password, nil
}
