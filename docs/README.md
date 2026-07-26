# Documentation

Documentation here is a developer level view of concepts and how the system is implemented.
While system operators and end users may benefit from this information, there are other docs which are more relevant to those audiences.

In particular:
* For Kubernetes Operator/OpenShift deployments, see the [CAO Documentation on CNG](https://docs.couchbase.com/operator/current/concept-cloud-native-gateway.html)
* For Capella Data API, see the [Runbook for Data API for Provisioned Clusters](https://confluence.issues.couchbase.com/wiki/x/S4ACv)

## Concepts

**Stellar Gateway** is a Couchbase Protostellar implementation that provides both a gRPC "protostellar" and a RESTful interface to Couchbase.  It is deployed 1:1 with a cluster, but may be deployed different ways.

### Architecture Overview

The system uses a modular architecture where each service (data, service discovery, Data API) can be independently configured and scaled. 

### Main Entry Points
- **`cmd/gateway/main.go`** - Main gateway service entry point with CLI interface
- **`cmd/dev/main.go`** - Development environment setup 
- **`cmd/bridge/main.go`** - Legacy bridge service (deprecated, never deployed)

### Core Modules

#### **Gateway Core (`gateway/`)**
- **`gateway.go`** - Main Gateway struct and orchestration logic
- **`system/`** - System-level server management (gRPC, HTTP, Data API servers)
- **`clustering/`** - Cluster membership and service discovery
- **`topology/`** - Network topology management

#### **Protocol Implementations**
- **`dataimpl/`** - Protostellar data API service (KV, Query, Search) implementation
- **`sdimpl/`** - Service discovery implementation  
- **`dapiimpl/`** - Data API v1 HTTP REST implementation with proxy services
- **`legacybridge/`** - Legacy protocol bridge for backwards compatibility

#### **Client**
- **`client/`** - Client front end for connecting to clusters
  - Connection management, routing, topology watching

#### **Data API Specification**
- **`dataapiv1/`** - OpenAPI spec and generated code for REST Data API
  - Auto-generated from `spec.yaml` using oapi-codegen

#### **Infrastructure Modules**
- **`auth/`** - Authentication (cbauth integration)
- **`ratelimiting/`** - Request rate limiting
- **`hooks/`** - Lifecycle hooks and barriers
- **`apiversion/`** - API versioning and gRPC interceptors

#### **Contrib/Utilities**
- **`contrib/`** - External components (etcd, goclustering, protostellar extensions)
- **`utils/`** - Utility packages (TLS, secrets management, networking)
- **`pkg/`** - Shared packages (metrics, interceptors, web API)

### Public Interfaces

1. **gRPC Protostellar API** - Main protocol interface (port 18098/18099)
2. **Data API REST** - HTTP REST API (configurable port, spec in `dataapiv1/`)
3. **Web/Metrics API** - Health checks and metrics (port 9091)
4. **Client Library** - Go client in `client/` package

### Key Features
- Multiple deployment modes (standalone, clustered)
- TLS support with configurable certificates
- Rate limiting and request throttling
- OpenTelemetry integration for observability
- Cloud provider secrets integration (AWS, Azure, GCP)
- Graceful shutdown and configuration reloading

## Detailed Documentation

* [Tuneables](tuneables.md)

