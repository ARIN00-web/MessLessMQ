ARG GO_VERSION=1.27.1
FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/messlessmq-broker ./cmd/broker \
    && mkdir -p /out/data \
    && chmod 0700 /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build --chown=nonroot:nonroot /out/messlessmq-broker /messlessmq-broker
COPY --from=build --chown=nonroot:nonroot /out/data /var/lib/messlessmq
ENV MESSLESSMQ_LISTEN_ADDRESS=:9092 \
    MESSLESSMQ_DATA_DIR=/var/lib/messlessmq \
    MESSLESSMQ_LOG_LEVEL=info
EXPOSE 9092
ENTRYPOINT ["/messlessmq-broker"]
