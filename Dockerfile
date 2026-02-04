FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go mod tidy && go build -o /logstream main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates iproute2 iputils netcat-openbsd

WORKDIR /root/

COPY --from=builder /logstream .

ENV NODE_ADDRESS=""
ENV IS_LEADER="false"
ENV MULTICAST_GROUP="239.0.0.1:9999"
ENV BROADCAST_PORT="8888"
ENV ANALYTICS_WINDOW_SECONDS="60"

EXPOSE 8001 8002 8888/udp 9999/udp

CMD ["./logstream"]