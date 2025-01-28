ARG BASE_IMAGE="alpine:3.20"

FROM golang:1.23.4-alpine3.20 AS build

RUN apk update && apk add build-base git

WORKDIR /worker

ARG GIT_COMMIT_HASH
ARG GIT_TAG

COPY go.mod ./
COPY go.sum ./
RUN go mod download

ADD . .

RUN CGO_ENABLED=1 go build \
  -ldflags="-extldflags=-static -extldflags=-ldl" \
  -tags netgo,osusergo \
  -o worker \
  .

FROM $BASE_IMAGE

ARG BASE_IMAGE
ARG GIT_COMMIT_HASH
ARG GIT_TAG
ARG BUILD_DATETIME

USER root
RUN adduser --disabled-password --gecos "" worker
RUN apk --no-cache add ca-certificates bash curl libstdc++ ncurses-libs

USER worker

COPY --from=build /worker/worker /usr/local/bin

ENTRYPOINT ["worker"]

LABEL orgs.opencontainers.image.created="$BUILD_DATETIME" \
      orgs.opencontainers.image.authors="Jorge Bay <jorgebaygondra@gmail.com>" \
      orgs.opencontainers.image.source="https://github.com/jorgebay/write-behind-cache-worker" \
      orgs.opencontainers.image.version="$GIT_TAG" \
      orgs.opencontainers.image.revision="$GIT_COMMIT_HASH" \
      orgs.opencontainers.image.title="Write-Behind Cache Worker" \
      orgs.opencontainers.image.description="A worker that reads from a backing SQL store and caches into Redis using write-behind strategy" \
      orgs.opencontainers.image.base.name="$BASE_IMAGE"
