
SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
.SECONDEXPANSION:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

ifeq ($(origin .RECIPEPREFIX), undefined)
  $(error This Make does not support .RECIPEPREFIX. Please use GNU Make 4.0 or later)
endif
.RECIPEPREFIX = >

# Run in parallel by default
ifneq "$(PARALLEL)" "false"
  MAKEFLAGS += --jobs --output-sync=target
endif

UNAME := $(shell uname)

ifeq ($(NEOBENCH_VERSION),)
  NEOBENCH_VERSION := dev
endif

build: tmp/.integration-tests-pass
.PHONY: build

tmp/.binaries-built: out/neobench_$(NEOBENCH_VERSION)_linux_amd64 \
  out/neobench_$(NEOBENCH_VERSION)_linux_arm64 \
  out/neobench_$(NEOBENCH_VERSION)_windows_amd64 \
  out/neobench_$(NEOBENCH_VERSION)_darwin_amd64
> mkdir --parents $(@D)
> touch $@

out/neobench_$(NEOBENCH_VERSION)_linux_amd64: tmp/.unit-tests-pass
> mkdir --parents $(@D)
> env GOOS=linux GOARCH=amd64 go build -o $@

out/neobench_$(NEOBENCH_VERSION)_linux_arm64: tmp/.unit-tests-pass
> mkdir --parents $(@D)
> env GOOS=linux GOARCH=arm64 go build -o $@

out/neobench_$(NEOBENCH_VERSION)_windows_amd64: tmp/.unit-tests-pass
> mkdir --parents $(@D)
> env GOOS=windows GOARCH=amd64 go build -o $@

out/neobench_$(NEOBENCH_VERSION)_darwin_amd64: tmp/.unit-tests-pass
> mkdir --parents $(@D)
> env GOOS=darwin GOARCH=amd64 go build -o $@

tmp/.unit-tests-pass: tmp/.go-vet
> mkdir --parents $(@D)
> go test ./...
> touch $@

tmp/.go-vet: tmp/.gofmt
> mkdir --parents $(@D)
> go vet ./...
> touch $@

tmp/.gofmt: $(shell find . -name '*.go')
> mkdir --parents $(@D)
> if [[ "$$(gofmt -l .)" != "" ]]; then
>   echo "You need to run gofmt on these files:"
>   gofmt -l .
>   exit 1
> fi
> touch $@

tmp/.integration-tests-pass: tmp/.binaries-built test/integration-test
> mkdir --parents $(@D)
> export NEOBENCH_PATH="$$(realpath out/neobench_$(NEOBENCH_VERSION)_linux_amd64)"
> NEO4J_IMAGE="neo4j:4.1.0-enterprise" test/integration-test
> touch $@

