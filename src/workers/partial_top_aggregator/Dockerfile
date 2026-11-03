FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/partial_top_aggregator ./src/workers/partial_top_aggregator

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -o bin/partial_top_aggregator ./src/workers/partial_top_aggregator

FROM busybox:latest
COPY --from=builder /build/bin/partial_top_aggregator /partial_top_aggregator
ENTRYPOINT ["./partial_top_aggregator"]
