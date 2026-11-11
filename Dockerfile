ARG GO_VERSION=1.27.1
FROM golang:${GO_VERSION}-alpine AS build

WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/messlessmq-broker ./cmd/broker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build --chown=nonroot:nonroot /out/messlessmq-broker /messlessmq-broker
EXPOSE 9092
ENTRYPOINT ["/messlessmq-broker"]
