FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/shared_words_aggregator ./src/workers/shared_words_aggregator

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -o bin/shared_words_aggregator ./src/workers/shared_words_aggregator

FROM busybox:latest
COPY --from=builder /build/bin/shared_words_aggregator /shared_words_aggregator
ENTRYPOINT ["./shared_words_aggregator"]
