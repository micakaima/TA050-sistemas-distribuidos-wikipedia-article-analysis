FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/client ./src/client

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/client ./src/client

FROM busybox:latest
COPY --from=builder /build/bin/client /client
ENTRYPOINT ["./client"]