#!/usr/bin/env bash
set -e

# exists exits with an error if the given command is not found
function exists() {
  if command -v $1 &>/dev/null; then
    return
  fi

  echo "command $1 not found, please ensure it's installed"

  exit 1
}

# Install with 'npm i -g swagger-cli'
exists "swagger-cli"

# Generate the Open API spec
swagger-cli bundle ./spec.yaml --outfile ./spec.generated.yaml --type yaml
go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen --config=./codegen.yaml ./spec.generated.yaml

# Validate the spec
swagger-cli validate ./spec.generated.yaml