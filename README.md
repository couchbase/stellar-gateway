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
> go generate
> go build -o stellar-nebula-gateway ./cmd/gateway
> ./stellar-nebula-gateway
```

### Executing the legacy bridge

```
> go generate
> go build ./cmd/bridge
> ./bridge
```

### Executing for development

This (by default) starts 3 instances of the gateway and 3 instances
of the legacy bridge running on the local system on default ports.

```
> go generate
> go run ./cmd/dev
```

## Help

If you encounter any issues, message Brett Lawson on Slack.
