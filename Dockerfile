FROM scratch
ARG TARGETARCH=amd64
COPY bin/linux/stellar-nebula-gateway-${TARGETARCH} /stellar-nebula-gateway
ENTRYPOINT ["/stellar-nebula-gateway"]