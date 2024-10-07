.PHONY: start
start:
	docker compose up -d

.PHONY: stop
stop:
	docker compose down

.PHONY: run
run:
	go run

.PHONY: test
test:
	go test -v ./...

.PHONY: build
build:
	go build
