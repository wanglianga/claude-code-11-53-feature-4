# syntax=docker/dockerfile:1.7
#
# Base images are ARGs so mirrored / air-gapped environments can override them,
# e.g.  docker compose build --build-arg GO_IMAGE=<mirror>/golang:1.26-bookworm
ARG GO_IMAGE=golang:1.26-bookworm
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian12:nonroot

FROM ${GO_IMAGE} AS build
WORKDIR /src

# Go module proxy mirror; the default matches this environment's GOPROXY.
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY} GOTOOLCHAIN=local

# Dependencies first for layer caching.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY web ./web
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/agrisync ./cmd/server

FROM ${RUNTIME_IMAGE}
WORKDIR /app
COPY --from=build /out/agrisync /app/agrisync
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/app/agrisync"]
