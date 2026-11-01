FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/word_counter ./src/workers/word_counter

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -o bin/word_counter ./src/workers/word_counter

FROM busybox:latest
COPY --from=builder /build/bin/word_counter /word_counter
ENTRYPOINT ["./word_counter"]