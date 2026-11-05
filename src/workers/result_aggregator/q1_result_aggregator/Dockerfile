FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/result_aggregator/q1_result_aggregator/ ./src/workers/result_aggregator/q1_result_aggregator

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -o bin/q1_result_aggregator ./src/workers/result_aggregator/q1_result_aggregator

FROM busybox:latest
COPY --from=builder /build/bin/q1_result_aggregator /result_aggregator/q1_result_aggregator
ENTRYPOINT ["./result_aggregator/q1_result_aggregator"]
