IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The native engine targets the scorer's Cascade Lake x86-64 CPUs.
PLATFORM ?= linux/amd64
DEV   ?= dev
# In the monorepo the fixtures live two levels up; the published starter has its own copy.
FIXTURES ?= $(if $(wildcard fixtures),fixtures,../../fixtures)
# Local-only override for emulators without AVX-512 (Apple silicon):
#   make fixtures ARCH_FLAGS=-march=haswell
# Leave it unset for anything you submit; the Dockerfile defaults to Cascade Lake.
ARCH_FLAGS ?=
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures generator dev run-dev check-dev

## Build the image for the scorer's platform, linux/amd64 (emulated on Apple silicon)
build:
	docker buildx build --platform $(PLATFORM) --load $(if $(ARCH_FLAGS),--build-arg ARCH_FLAGS="$(ARCH_FLAGS)") -t $(IMAGE) .

## Run the image on all 36 edge-case fixtures and diff against the expected tables
fixtures: build
	python3 scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

## Build the published generator (requires Rust 1.85+)
generator:
	cargo build --release --manifest-path tools/generator/Cargo.toml

## Make the official 1M-event development set.
dev: generator
	tools/generator/target/release/generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Compare your dev output with the published dev answer (dev-answer.zip, unzipped): make check-dev ANSWER=dev-answer
ANSWER ?= tools/dev-answer
check-dev:
	python3 scripts/compare.py out $(ANSWER)
