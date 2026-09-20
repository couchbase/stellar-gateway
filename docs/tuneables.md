# Tuneables

## Rate Limiting

In a Capella deployment, we wish to have a rate limit as a simple method of avoiding accidental overload, resource exhaustion and pathological system failure.
Comprehensive DDoS prevention is more complex and outside the scope of what Data API can provide, but this feature provides some reasonable limit.

**Field:** `rate-limit`

**Format:** Integer, unsigned

**Default:** 0 (unlimited). Capella deployments default this to 10000.

**Description:** A per-process limit which, when exceeded, will return HTTP 429 errors.

This is service independent and resource usage/complexity of underlying services called may not align. For example, KV operations with options like durability, complex queries, and FTS requests may exhaust resources before this rate limit is hit.

**When to adjust:** 
- If a deployment wants to be more conservative on Data API or CNG resource usage, this may be tuned lower which will reject requests to keep load lower.
- If a deployment wants to allow more resource usage, increase this to a higher level or to the value 0 to be unlimited.