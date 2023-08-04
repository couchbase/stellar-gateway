FROM scratch
ARG TARGETARCH
COPY bin/linux/cloud-native-gateway-${TARGETARCH} /cloud-native-gateway
ENTRYPOINT ["/cloud-native-gateway"]