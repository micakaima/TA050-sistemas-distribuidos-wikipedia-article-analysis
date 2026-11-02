FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/old_articles_averager ./src/workers/old_articles_averager

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -o bin/old_articles_averager ./src/workers/old_articles_averager

FROM busybox:latest
COPY --from=builder /build/bin/old_articles_averager /old_articles_averager
ENTRYPOINT ["./old_articles_averager"]