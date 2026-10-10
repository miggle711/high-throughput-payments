# Build any service: docker build --build-arg SERVICE=payment-service -t payment-service .
# Build the migrations image: docker build --target migrate -t migrate .
FROM golang:1.25.5-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE
RUN CGO_ENABLED=0 go build -o /out/service ./${SERVICE}

FROM migrate/migrate:v4.18.1 AS migrate
COPY internal/db/migrations /migrations

FROM gcr.io/distroless/static-debian12:nonroot AS service
COPY --from=build /out/service /service
ENTRYPOINT ["/service"]
