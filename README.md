# Stellar Nebula

A protostellar implementation.

## Description

Provides translation capabilities for Protostellar to legacy protocols.

## Getting Started

### Dependencies

- Go @ v1.18+
- protoc @ v3+
- protoc-gen-go @ v1.28+
- protoc-gen-go-grpc @ v1.2+

### Executing the gateway

```
> git submodule update --recursive --init
> go generate
> go build ./cmd/gateway
> ./gateway
```

### Executing the legacy bridge

Note: This is not currently implemented.

```
> git submodule update --recursive --init
> go generate
> go build ./cmd/bridge
> ./bridge
```

### Executing for development

This (by default) starts 3 instances of the gateway and 3 instances
of the legacy bridge running on the local system on default ports.

```
> git submodule update --recursive --init
> go generate
> go run ./cmd/dev
```

## Help

If you encounter any issues, message Brett Lawson on Slack.
