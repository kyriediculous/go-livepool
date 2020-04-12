module github.com/kyriediculous/go-livepool

go 1.13

require (
	contrib.go.opencensus.io/exporter/prometheus v0.1.0
	github.com/aws/aws-sdk-go v1.23.19
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/ethereum/go-ethereum v1.9.3
	github.com/flyingmutant/rapid v0.0.0-20190904072629-5761511f78c8
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/protobuf v1.3.2
	github.com/livepeer/go-livepeer v0.5.5
	github.com/livepeer/lpms v0.0.0-20200110164555-e34a4737b857
	github.com/livepeer/m3u8 v0.11.0
	github.com/mattn/go-sqlite3 v1.11.0
	github.com/olekukonko/tablewriter v0.0.1
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.1.0
	github.com/stretchr/testify v1.4.0
	github.com/urfave/cli v1.20.0
	go.opencensus.io v0.22.1
	go.uber.org/goleak v1.0.0
	golang.org/x/net v0.0.0-20190909003024-a7b16738d86b
	google.golang.org/grpc v1.23.0
)

replace gopkg.in/urfave/cli.v1 => github.com/urfave/cli v1.22.2-0.20191002033821-63cd2e3d6bb5
