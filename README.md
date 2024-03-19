# Stellar Nebula

A protostellar implementation.

## Description

Provides translation capabilities for Protostellar to legacy protocols.

## Getting Started

### Dependencies

- Go @ v1.19+
- protoc @ v3+
- protoc-gen-go @ v1.28+
- protoc-gen-go-grpc @ v1.2+

### Executing the gateway

Building the gateway:

```
> go build -o stellar-gateway ./cmd/gateway
```

Basic execution which will run generate a self-signed certificate and run against a Couchbase Server instance on localhost:
```
> ./stellar-gateway --self-sign
```

To turn up the log level:
```
> ./stellar-gateway --self-sign --log-level debug
```

To add extra debug information to errors:
```
> ./stellar-gateway --self-sign --debug
```

To execute against a remote Couchbase Server instance:
```
> ./stellar-gateway --self-sign --cb-host 192.168.107.128
```

To execute using an existing certificate (used to connect to the gateway by clients): 
```
> ./stellar-gateway --cert /path/to/cert --key /path/to/key
```

To execute without having to perform a build after every change
```
> go run ./cmd/gateway --self-sign
```

## Help

```
> ./stellar-gateway --help
```

If you encounter any issues, drop a message into #protostellar on Slack.
