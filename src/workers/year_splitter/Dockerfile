FROM golang:1.27 AS builder

WORKDIR /build/

COPY go.mod go.sum ./
RUN go mod download

COPY src/common ./src/common
COPY src/workers/year_splitter ./src/workers/year_splitter

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux go build -o bin/year_splitter ./src/workers/year_splitter

FROM busybox:latest
COPY --from=builder /build/bin/year_splitter /year_splitter
ENTRYPOINT ["./year_splitter"]