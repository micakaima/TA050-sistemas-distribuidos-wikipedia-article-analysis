FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/gateway ./src/gateway

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/gateway ./src/gateway

FROM busybox:latest
COPY --from=builder /build/bin/gateway /gateway
ENTRYPOINT ["./gateway"]