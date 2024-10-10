# Write-Behind Cache Worker

A worker that reads from a backing SQL store and caches into Redis using write-behind strategy

[![Go](https://github.com/jorgebay/write-behind-cache-worker/actions/workflows/test.yml/badge.svg)](https://github.com/jorgebay/write-behind-cache-worker/actions/workflows/test.yml)

## Building

```shell
make build
```

## Running tests

```shell
make start
make test
```
