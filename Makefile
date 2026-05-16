.PHONY: all test lint fmt clean build bump-version

COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS := -X 'github.com/ThreadifyDev/go-sdk.Commit=$(COMMIT)'

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

bump-version:
	@test -n "$(VERSION)" || (echo "usage: make bump-version VERSION=0.2.1" && exit 1)
	@printf '%s\n' "$(VERSION)" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$$' || (echo "VERSION must be semver without a leading v, e.g. 0.2.1" && exit 1)
	@printf '%s\n' "$(VERSION)" > VERSION
	@echo "Updated VERSION to $(VERSION)"
