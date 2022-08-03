all: lint test build

test:
	go test ./...

lint:
	go vet
	golangci-lint run

check: generate lint test

generate: genproto
	go generate

build: generate
	go build ./

.PHONY: all test lint check generate build