FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/english_filter ./src/workers/english_filter

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/english_filter ./src/workers/english_filter

FROM busybox:latest
COPY --from=builder /build/bin/english_filter /english_filter
ENTRYPOINT ["./english_filter"]