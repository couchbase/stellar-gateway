# topology

The topology package provides local topology handling. This keeps track
of which nodes exist within this nebula cluster and allows us to properly
distribute requests among a group of nebula nodes.

## staticprovider

This provider simply servers the local node only.

## etcdprovider

This provider uses ETCD to manage the list of nodes for this cluster.

## nsprovider

This provider uses ns_server as a source-of-truth and simply mirrors the topology
of the cluster onto Protostellar.
