FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/above_average_filter ./src/workers/above_average_filter

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -o bin/above_average_filter ./src/workers/above_average_filter

FROM busybox:latest
COPY --from=builder /build/bin/above_average_filter /above_average_filter
ENTRYPOINT ["./above_average_filter"]