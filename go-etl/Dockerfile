FROM golang:1.27-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/etl ./cmd/etl

FROM alpine:3.22 AS runtime

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S etl \
    && adduser -S -G etl -h /app etl

WORKDIR /app
COPY --from=build /out/etl /usr/local/bin/etl
COPY schema.sql schema_loadtest.sql /app/

USER etl

EXPOSE 8081

ENTRYPOINT ["/usr/local/bin/etl"]
CMD ["serve", "--config", "/app/config/app.toml"]
