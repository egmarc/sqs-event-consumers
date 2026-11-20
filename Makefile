.PHONY: test vet build build-DBWorker build-S3Worker build-Reconciler

test:
	go test ./...

vet:
	go vet ./...

build:
	go build ./cmd/...

build-DBWorker:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$(ARTIFACTS_DIR)/bootstrap" ./cmd/db-worker
	cp telemetry/collector.yaml "$(ARTIFACTS_DIR)/collector.yaml"

build-S3Worker:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$(ARTIFACTS_DIR)/bootstrap" ./cmd/s3-worker
	cp telemetry/collector.yaml "$(ARTIFACTS_DIR)/collector.yaml"

build-Reconciler:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$(ARTIFACTS_DIR)/bootstrap" ./cmd/reconciler
	cp telemetry/collector.yaml "$(ARTIFACTS_DIR)/collector.yaml"
