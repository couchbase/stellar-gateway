ARG BASE_IMAGE=scratch

FROM ${BASE_IMAGE}

ARG TARGETARCH
ARG PROD_VERSION=0.0.0
ARG OS_BUILD=

COPY LICENSE /licenses/couchbase-cloud-native-gateway.txt
COPY bin/linux/cloud-native-gateway-${TARGETARCH} /cloud-native-gateway

USER 8459

LABEL name="couchbase/cloud-native-gateway" \
    vendor="Couchbase" \
    maintainer="Couchbase" \
    version="${PROD_VERSION}" \
    release="${OS_BUILD}" \
    summary="Couchbase Cloud Native Gateway ${PROD_VERSION}" \
    description="Couchbase Cloud Native Gateway ${PROD_VERSION}" \
    io.k8s.description="The Couchbase Cloud Native Gateway."

ENTRYPOINT ["/cloud-native-gateway"]
