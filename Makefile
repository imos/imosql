export GOPATH = $(shell pwd | sed -e 's/\/src\/.*$$//g')

test: build
	go test -v
.PHONY: test

integration-test: build
	go test -v --enable_integration_test
.PHONY: integration-test

build: get
	go build
.PHONY: build

get: version
	go get
.PHONY: get

version:
	@go version
.PHONY: version

format:
	gofmt -w ./
.PHONY: format

info:
	@echo "GOPATH=$${GOPATH}"
.PHONY: info
