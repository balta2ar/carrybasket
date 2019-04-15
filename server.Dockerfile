FROM golang:1.12.3-alpine AS builder

RUN apk update && apk add --no-cache git ca-certificates tzdata && update-ca-certificates

RUN mkdir /app
WORKDIR /app

COPY . /app
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o /bin/carrybasket_server server/main.go

FROM scratch

COPY --from=builder /bin/carrybasket_server /bin/carrybasket_server

ENTRYPOINT ["/bin/carrybasket_server"]
