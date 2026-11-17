.PHONY: test vet build build-DBWorker build-S3Worker

test:
	go test ./...

vet:
	go vet ./...

build:
	go build ./cmd/...

build-DBWorker:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$(ARTIFACTS_DIR)/bootstrap" ./cmd/db-worker

build-S3Worker:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$(ARTIFACTS_DIR)/bootstrap" ./cmd/s3-worker
