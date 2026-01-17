FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /logstream main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates iproute2 iputils

WORKDIR /root/

COPY --from=builder /logstream .

ENV NODE_ADDRESS=""
ENV IS_LEADER="false"
ENV MULTICAST_GROUP="239.0.0.1:9999"
ENV BROADCAST_PORT="8888"

EXPOSE 8001 8002 8888/udp 9999/udp

CMD ["./logstream"]