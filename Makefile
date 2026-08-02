GO        ?= go
BIN_DIR   := bin
PKGS      := ./...

.PHONY: all build test test-integration lint fmt tidy clean

all: lint test build

build:
	$(GO) build -o $(BIN_DIR)/ ./cmd/...

test:
	$(GO) test -race -count=1 $(PKGS)

# Integration tests spin up Postgres / Redis / Kafka with testcontainers and
# therefore need a running Docker daemon.
test-integration:
	$(GO) test -race -count=1 -tags=integration -timeout=10m $(PKGS)

lint:
	$(GO) vet $(PKGS)
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

fmt:
	gofmt -w .

tidy:
	$(GO) mod tidy

clean:
	rm -rf $(BIN_DIR)
