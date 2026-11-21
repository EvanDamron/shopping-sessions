IMAGE ?= shopping-sessions:latest
PLATFORM ?= linux/amd64
export IMAGE PLATFORM

.PHONY: build release fixtures check

release:
	cargo build --release

## Build the scorer image (linux/amd64)
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Native binary: 36 fixtures, dev answer, and reference outputs for extra seeds
check: release
	scripts/check_all.sh

## The image on all 36 fixtures, the way the scorer runs it
fixtures: build
	uv run -q --with polars python scripts/run_fixtures.py fixtures -- scripts/docker_run.sh
