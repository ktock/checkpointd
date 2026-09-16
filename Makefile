.PHONY: all build proto test test-k8s test-python clean install deps clean-logs

# Build all binaries
all: proto build

# Build binaries
build:
	@mkdir -p bin
	@echo "Building checkpointd..."
	@go build -o bin/checkpointd ./cmd/checkpointd
	@echo "Build complete!"


# Generate protobuf code
proto:
	@echo "Generating protobuf code..."
	@export PATH=$$PATH:$$(go env GOPATH)/bin && \
		protoc --go_out=. --go_opt=paths=source_relative \
		       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
		       proto/ax.proto proto/content.proto
# Python path is unimplemented as of now
#	@python3 -m grpc_tools.protoc -I. --python_out=python --grpc_python_out=python proto/ax.proto proto/content.proto
	@$$(go env GOPATH)/bin/addlicense -l apache python/proto/*.py
	@echo "Protobuf generation complete!"

# Run Go tests
test:
	@echo "Running Go tests..."
	@go test -v ./...

# Full end-to-end test against an ephemeral kind cluster. Set
# TEST_K8S_TARGET to a space-separated list of subtest ids (see
# script/test-k8s/subtests/) to run just those, e.g.:
#   TEST_K8S_TARGET=tck make test-k8s
test-k8s:
	@echo "Running checkpointd k8s end-to-end test..."
	@./script/test-k8s/test.sh

# Run Python tests for the antigravity harness sidecar.
# Assumes deps are installed for the same interpreter as `python3`, e.g.:
#   python3 -m pip install -r python/antigravity/requirements.txt \
#     'pytest>=7.0' 'pytest-timeout>=2.0'
# --timeout guards against hung gRPC servers.
test-python:
	@echo "Running Python tests..."
	@python3 -m pytest python/antigravity/ --timeout=30 --timeout-method=thread

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -rf eventlog/
	@echo "Clean complete!"

# Install checkpointd to GOPATH/bin
install:
	@echo "Installing checkpointd..."
	@go install ./cmd/checkpointd
	@echo "Install complete!"

# Install dependencies
deps:
	@echo "Installing dependencies..."
	@go mod download
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@go install github.com/google/addlicense@latest
	@echo "Dependencies installed!"

clean-logs:
	@echo "Cleaning the event logs..."
	rm -rf ./eventlog
	mkdir ./eventlog
