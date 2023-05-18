FROM scratch
ARG TARGETARCH=amd64
COPY bin/linux/cloud-native-gateway-${TARGETARCH} /cloud-native-gateway
ENTRYPOINT ["/cloud-native-gateway"]