FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/year_filter ./src/workers/year_filter

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/year_filter ./src/workers/year_filter

FROM busybox:latest
COPY --from=builder /build/bin/year_filter /year_filter
ENTRYPOINT ["./year_filter"]