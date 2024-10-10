# Write-Behind Cache Worker

A worker that reads from a backing SQL store and caches into Redis using write-behind strategy.

[![Go](https://github.com/jorgebay/write-behind-cache-worker/actions/workflows/test.yml/badge.svg)](https://github.com/jorgebay/write-behind-cache-worker/actions/workflows/test.yml)

## Features

- Polls from the db at regular intervals
- Uses Redis request pipeline
- Tolerates intermittent failures
- Easy to define the db queries and templates for redis keys
- Configurable via env vars or config file

## Building

```shell
make build
```

## Running tests

```shell
make start
make test
```

## License

This software may be modified and distributed under the terms of the MIT license. See the LICENSE file for details.
