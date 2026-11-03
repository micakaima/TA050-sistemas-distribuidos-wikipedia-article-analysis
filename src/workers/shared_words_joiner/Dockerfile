FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/shared_words_joiner ./src/workers/shared_words_joiner

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -o bin/shared_words_joiner ./src/workers/shared_words_joiner

FROM busybox:latest
COPY --from=builder /build/bin/shared_words_joiner /shared_words_joiner
ENTRYPOINT ["./shared_words_joiner"]
