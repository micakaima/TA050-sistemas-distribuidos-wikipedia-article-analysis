FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/managers/column_filter_manager ./src/managers/column_filter_manager

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -o bin/column_filter_manager ./src/managers/column_filter_manager

FROM busybox:latest
COPY --from=builder /build/bin/column_filter_manager /column_filter_manager
ENTRYPOINT ["./column_filter_manager"]