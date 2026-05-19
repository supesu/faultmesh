# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26
FROM golang:${GO_VERSION}-alpine AS build

ARG CMD
ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=0 GOOS=linux

RUN go build \
  -ldflags="-s -w \
    -X 'github.com/supesu/faultmesh/data-plane/pkg/version.Version=${VERSION}' \
    -X 'github.com/supesu/faultmesh/data-plane/pkg/version.GitCommit=${COMMIT}' \
    -X 'github.com/supesu/faultmesh/data-plane/pkg/version.BuildDate=${BUILD_DATE}'" \
  -o /out/app ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot AS runtime

COPY --from=build /out/app /app

USER nonroot:nonroot
ENTRYPOINT ["/app"]
