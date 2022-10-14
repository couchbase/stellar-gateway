# topology

The topology package provides local topology handling. This keeps track
of which nodes exist within this nebula cluster and allows us to properly
distribute requests among a group of nebula nodes.

## InProcProvider

This provider simply servers the local node only.

## EtcdProvider

This provider uses ETCD to manage the list of nodes for this cluster.
