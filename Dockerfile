# syntax=docker/dockerfile:1.7
#
# One Dockerfile for every service; pick the binary with --build-arg SERVICE:
#   docker build --build-arg SERVICE=api    -t coldchain/api .
#   docker build --build-arg SERVICE=ingest -t coldchain/ingest .

FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG SERVICE
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

# Dependencies first so they are cached independently of source changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    test -n "$SERVICE" && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

# distroless: no shell or package manager, runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/app"]
