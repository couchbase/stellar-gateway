module github.com/couchbase/stellar-gateway

go 1.18

require (
	github.com/cenkalti/backoff/v4 v4.1.3
	github.com/couchbase/cbauth v0.1.7
	github.com/couchbase/gocb/v2 v2.5.2
	github.com/couchbase/gocbcore/v10 v10.2.0
	github.com/couchbase/gocbcorex v0.0.0-20230221114919-ef6a7a7da721
	github.com/couchbase/goprotostellar v0.0.0
	github.com/couchbaselabs/gocbconnstr v1.0.5
	github.com/fsnotify/fsnotify v1.6.0
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.1
	github.com/spf13/cobra v1.7.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.15.0
	github.com/stretchr/testify v1.8.1
	go.etcd.io/etcd/api/v3 v3.5.6
	go.etcd.io/etcd/client/v3 v3.5.6
	go.uber.org/zap v1.24.0
	golang.org/x/exp v0.0.0-20230213192124-5e25df0256eb
	google.golang.org/genproto v0.0.0-20230131230820-1c016267d619
	google.golang.org/grpc v1.52.3
	google.golang.org/protobuf v1.28.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/couchbase/clog v0.1.0 // indirect
	github.com/couchbase/go-couchbase v0.1.1 // indirect
	github.com/couchbase/gomemcached v0.2.1 // indirect
	github.com/couchbase/goutils v0.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/pelletier/go-toml/v2 v2.0.6 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/spf13/afero v1.9.3 // indirect
	github.com/spf13/cast v1.5.0 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/subosito/gotenv v1.4.2 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.6 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	golang.org/x/crypto v0.5.0 // indirect
	golang.org/x/net v0.5.0 // indirect
	golang.org/x/sys v0.4.0 // indirect
	golang.org/x/text v0.6.0 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/couchbase/goprotostellar => ./contrib/goprotostellar

replace github.com/couchbase/gocbcorex => ./contrib/gocbcorex

// use the forked cbauth with STG fixes for now
replace github.com/couchbase/cbauth => github.com/brett19/cbauth v0.0.0-20230203045612-2a680fc7c1e5
