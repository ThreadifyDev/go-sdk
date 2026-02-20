.PHONY: all test lint fmt clean build

COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS := -X 'github.com/threadify/threadify-sdk-go.Commit=$(COMMIT)'

all: lint test

build:
	go build -ldflags="$(LDFLAGS)" -o bin/sdk ./...

test:
	go test -v ./...

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run

fmt:
	go fmt ./...

clean:
	go clean
	rm -f bin/*
