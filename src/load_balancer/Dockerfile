FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/load_balancer ./src/load_balancer

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/load_balancer ./src/load_balancer

FROM busybox:latest
COPY --from=builder /build/bin/load_balancer /load_balancer
ENTRYPOINT ["./load_balancer"]