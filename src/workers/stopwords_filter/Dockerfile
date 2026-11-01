FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/stopwords_filter ./src/workers/stopwords_filter

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -o bin/stopwords_filter ./src/workers/stopwords_filter

FROM busybox:latest
COPY --from=builder /build/bin/stopwords_filter /stopwords_filter
ENTRYPOINT ["./stopwords_filter"]