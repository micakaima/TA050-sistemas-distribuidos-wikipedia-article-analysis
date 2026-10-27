FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/bio_filter ./src/workers/bio_filter

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/bio_filter ./src/workers/bio_filter

FROM busybox:latest
COPY --from=builder /build/bin/bio_filter /bio_filter
ENTRYPOINT ["./bio_filter"]