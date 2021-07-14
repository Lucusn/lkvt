module lucusn.io/lkvt

replace github.com/coreos/bbolt v1.3.4 => go.etcd.io/bbolt v1.3.4

replace go.etcd.io/bbolt v1.3.4 => github.com/coreos/bbolt v1.3.4

replace niovakv/clientapi => ../

replace niovakv/serfclienthandler => ../../serf/client

replace niovakv/niovakvlib => ../../lib

replace niovakv/httpclient => ../../http/client

go 1.16

require (
	github.com/aybabtme/uniplot v0.0.0-20151203143629-039c559e5e7e // indirect
	github.com/cncf/udpa/go v0.0.0-20201120205902-5459f2c99403 // indirect
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/envoyproxy/go-control-plane v0.9.5 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/google/go-cmp v0.5.0 // indirect
	github.com/google/uuid v1.2.0 // indirect
	github.com/sirupsen/logrus v1.8.1
	go.etcd.io/etcd v3.3.25+incompatible
	go.uber.org/zap v1.17.0 // indirect
	google.golang.org/grpc v1.26.0 // indirect
	niovakv/clientapi v0.0.0-00010101000000-000000000000 // indirect
	niovakv/niovakvlib v0.0.0-00010101000000-000000000000 // indirect
)
