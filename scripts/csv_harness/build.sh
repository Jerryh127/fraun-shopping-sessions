#!/bin/sh
# Engine with CSV output instead of Parquet, so it builds without Arrow.
# Usage: scripts/csv_harness/build.sh OUT   (ARCH_FLAGS overrides the target CPU)
set -eu
here=$(cd "$(dirname "$0")" && pwd); src=$here/../../native
${CXX:-g++} -std=c++20 -O3 -DNDEBUG ${ARCH_FLAGS:--march=cascadelake -mtune=cascadelake} -flto=auto -pthread -I"$src" \
  "$src"/main.cpp "$src"/links.cpp "$src"/ingest.cpp "$src"/aggregate.cpp "$here"/output_csv.cpp -o "$1"
