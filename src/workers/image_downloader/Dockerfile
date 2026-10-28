FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/image_downloader ./src/workers/image_downloader

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -o bin/image_downloader ./src/workers/image_downloader

FROM busybox:latest
COPY --from=builder /build/bin/image_downloader /image_downloader
ENTRYPOINT ["./image_downloader"]