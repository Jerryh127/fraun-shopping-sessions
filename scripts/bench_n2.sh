#!/bin/sh
# Stage timings on the scorer's machine type. Run from the repo root on a fresh
# Debian 12 n2-standard-8 VM (50 GB disk). Prints the kernel's huge-page setting,
# then per-stage times for a 20M-event and a 60M-event dataset under the scorer's
# container limits. Usage: sh scripts/bench_n2.sh [runs-per-dataset, default 5]
set -eu
RUNS=${1:-5}
command -v docker >/dev/null || { sudo apt-get update -qq && sudo apt-get install -y -qq docker.io; }
D="sudo docker"

echo "== machine"
lscpu | grep -E 'Model name|^CPU\(s\)|Thread|L2|L3'
echo "transparent_hugepage: $(cat /sys/kernel/mm/transparent_hugepage/enabled)"
free -g | sed -n 2p

echo "== build"
$D build -q -t shopping-sessions:latest .
$D run --rm -v "$PWD":/w -w /w rust:1.85-slim sh -c '
  cargo build -q --release --manifest-path tools/generator/Cargo.toml &&
  G=tools/generator/target/release/generate &&
  { [ -d bench/p20 ] || $G --seed 3 --events 20000000 --clicks 5000000 --output bench/p20; } &&
  { [ -d bench/p60 ] || $G --seed 4 --events 60000000 --clicks 15000000 --output bench/p60; }' >/dev/null

run() {  # run DATASET -> stage lines on stderr of the container, wall time from the host
  out=$(mktemp -d); scratch=$(mktemp -d); chmod 777 "$out" "$scratch"
  start=$(date +%s.%N)
  $D run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
    --cpus 8 --memory 24g --memory-swap 24g --user 1000:1000 \
    -v "$PWD/bench/$1":/data:ro -v "$out":/output -v "$scratch":/scratch \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
    -e NCPU=8 -e MEM_GB=24 -e SHOP_PROFILE=1 shopping-sessions:latest 2>&1 | tr '\n' ' '
  end=$(date +%s.%N)
  echo "| docker-run wall $(echo "$end - $start" | bc) s | output $(du -sm "$out" | cut -f1) MB"
  sudo rm -rf "$out" "$scratch"
}
command -v bc >/dev/null || sudo apt-get install -y -qq bc
for set in p20 p60; do
  echo "== $set (first run includes reading the files from disk)"
  i=0; while [ $i -lt "$RUNS" ]; do run $set; i=$((i+1)); done
done
