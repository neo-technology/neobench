
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

build: tmp/.integration-tests-pass
.PHONY: build

tmp/.binaries-built: out/neobench_linux_amd64 out/neobench_linux_arm64 \
  out/neobench_windows_amd64 out/neobench_darwin_amd64
> mkdir --parents $(@D)
> touch $@

out/neobench_linux_amd64: tmp/.unit-tests-pass
> mkdir --parents $(@D)
> env GOOS=linux GOARCH=amd64 go build -o $@

out/neobench_linux_arm64: tmp/.unit-tests-pass
> mkdir --parents $(@D)
> env GOOS=linux GOARCH=arm64 go build -o $@

out/neobench_windows_amd64: tmp/.unit-tests-pass
> mkdir --parents $(@D)
> env GOOS=windows GOARCH=amd64 go build -o $@

out/neobench_darwin_amd64: tmp/.unit-tests-pass
> mkdir --parents $(@D)
> env GOOS=darwin GOARCH=amd64 go build -o $@

tmp/.unit-tests-pass: $(shell find . -name '*.go')
> mkdir --parents $(@D)
> go test ./...
> touch $@

tmp/.integration-tests-pass: tmp/.binaries-built
> mkdir --parents $(@D)
> export NEOBENCH_PATH="$$(realpath out/neobench_linux_amd64)"
> NEO4J_IMAGE="neo4j:4.1.0" test/integration-test
> touch $@

