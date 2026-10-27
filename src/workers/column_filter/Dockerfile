FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/column_filter ./src/workers/column_filter

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/column_filter ./src/workers/column_filter

FROM busybox:latest
COPY --from=builder /build/bin/column_filter /column_filter
ENTRYPOINT ["./column_filter"]