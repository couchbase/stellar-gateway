# API Stability and Versioning

We use the alpha, beta, ga terminology to describe versions, while using stability levels such
as volatile, uncommitted and committed to describe individual RPCs. Any versions or RPCs which
do not describe their level are assumed to be committed / ga.

Additionally, a correlation exists between the RPC stability and the versions it can exist
within, such that:

- Alpha maps directly to volatile.
- Beta maps directly to uncommitted.
- GA maps directly to committed.

The following is a list of definitions for each stability level within our gRPC and REST
interfaces:

## Alpha Endpoints

Alpha endpoints are experimental and intended for initial testing. They are not guaranteed to be stable and may change significantly without notice.

- API Stability: Poor
- Testing Coverage: Poor

## Beta Endpoints

Beta endpoints are generally stable and suitable for integration testing, but they might still undergo minor changes. The API is considered mostly final, but isn't yet fully production-ready.

- API Stability: Strong
- Testing Coverage: Fair

## General Availability (GA) Endpoints

GA endpoints are stable, fully tested, and ready for production use. These APIs will not change unless absolutely necessary, and any changes would be considered a major update.

- API Stability: Strong
- Testing Coverage: Strong
