FROM golang:1.23-alpine AS build

WORKDIR /src

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/trendsd ./cmd/trendsd

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/loadgen ./cmd/loadgen


FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/trendsd /trendsd
COPY --from=build /out/loadgen /loadgen

EXPOSE 8080
USER nonroot:nonroot

ENTRYPOINT ["/trendsd"]
