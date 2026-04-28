FROM golang:1.26-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -mod=vendor -o /lineage-service ./cmd/server

FROM alpine:3.21
COPY --from=build /lineage-service /lineage-service
ENV PORT=8090
EXPOSE 8090
ENTRYPOINT ["/lineage-service"]
