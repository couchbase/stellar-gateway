package dataapiv1

// oapi-codegen is run at a pinned version outside of this module so that its
// dependencies (notably github.com/dprotaso/go-yit and the yaml-jsonpath
// libraries) are resolved from its own go.mod rather than being upgraded
// alongside the gateway's dependencies.
//
// Generated code depends on github.com/oapi-codegen/runtime, so when bumping
// the version here, also bump the runtime in go.mod to a compatible release.
//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.8.0 --config=./codegen.yaml ./spec.yaml
