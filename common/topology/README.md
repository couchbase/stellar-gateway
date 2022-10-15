# topology

The topology package provides centralized handling of the local and remote topologies
with which the individual topologies for legacy and protostellar are generated. There
are multiple implementations of this depending on requirements.

### statelessprovider

This provider simply adds the revisions of the two input configurations together.

### etcdprovider

NOT YET IMPLEMENTED

This uses etcd to keep a consistent and stable configuration that can be used by all
the individual members of the cluster.
