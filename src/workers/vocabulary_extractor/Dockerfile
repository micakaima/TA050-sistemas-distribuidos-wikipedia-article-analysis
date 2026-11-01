FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/vocabulary_extractor ./src/workers/vocabulary_extractor

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -o bin/vocabulary_extractor ./src/workers/vocabulary_extractor

FROM busybox:latest
COPY --from=builder /build/bin/vocabulary_extractor /vocabulary_extractor
ENTRYPOINT ["./vocabulary_extractor"]