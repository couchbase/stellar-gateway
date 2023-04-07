SOURCE = $(shell find . -name *.go -type f)
bldNum = $(if $(BLD_NUM),$(BLD_NUM),9999)
version = $(if $(VERSION),$(VERSION),1.0.0)
productVersion = $(version)-$(bldNum)
# The git revision, infinitely more useful than an arbitrary build number.
REVISION := $(shell git rev-parse HEAD)

ARTIFACTS = build/artifacts/couchbase-stellar-gateway
DOCKER_TAG = v1
DOCKER_USER = couchbase
GOPATH := $(shell go env GOPATH)
GOBIN := $(if $(GOPATH),$(GOPATH)/bin,$(HOME)/go/bin)
GOLINT_VERSION := v1.50.1

# These are propagated into each binary so we can tell for sure the exact build
# that a binary came from.
LDFLAGS = \
  -s -w \
  -X github.com/couchbase/stellar-gateway/pkg/version.Version=$(version) \
  -X github.com/couchbase/stellar-gateway/pkg/version.BuildNumber=$(bldNum) \
  -X github.com/couchbase/stellar-gateway/pkg/revision.gitRevision=$(REVISION)

all: lint test build container

test:
	go test ./...

fmt:
	go install golang.org/x/tools/cmd/goimports@latest
	goimports -w .
	find . -name go.mod -execdir go mod tidy \;

lint:
	go vet
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLINT_VERSION)
	$(GOBIN)/golangci-lint run

check: generate fmt lint test

$(GOBIN)/protoc-gen-go-grpc:
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

$(GOBIN)/protoc-gen-go:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28

generate: $(GOBIN)/protoc-gen-go $(GOBIN)/protoc-gen-go-grpc
	PATH=$(GOBIN):$(PATH) go generate

build: generate
	for platform in linux darwin ; do \
	 for arch in amd64 arm64 ; do \
	   echo "Building $$platform $$arch binary " ; \
	   GOOS=$$platform GOARCH=$$arch CGO_ENABLED=0 GO11MODULE=on go build -o bin/$$platform/stellar-nebula-gateway-$$arch -ldflags="$(LDFLAGS)" ./cmd/gateway ; \
	 done \
	done

image-artifacts: build
	mkdir -p $(ARTIFACTS)/bin/linux
	cp bin/linux/stellar-nebula-* $(ARTIFACTS)/bin/linux
	cp Dockerfile* README.md LICENSE $(ARTIFACTS)

dist: image-artifacts
	rm -rf dist
	mkdir -p dist
	tar -C $(ARTIFACTS)/.. -czf dist/couchbase-stellar-gateway-image_$(productVersion).tgz .

container: build
	docker build -f Dockerfile -t ${DOCKER_USER}/stellar-gateway:${DOCKER_TAG} .

.PHONY: all test fmt lint check generate build