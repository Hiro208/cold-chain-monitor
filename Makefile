GO        ?= go
BIN_DIR   := bin
PKGS      := ./...

.PHONY: all build test test-integration lint fmt tidy clean up down logs

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

# Build the alert Lambda for the provided.al2023 arm64 (Graviton) runtime.
.PHONY: lambda
lambda:
	cd lambda/breachalert && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
		$(GO) build -tags lambda.norpc -trimpath -ldflags='-s -w' -o bootstrap . && \
		zip -q breachalert.zip bootstrap

# Local stack (requires Docker).
up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f ingest api
