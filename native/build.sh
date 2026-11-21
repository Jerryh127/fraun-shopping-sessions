#!/bin/sh
set -eu
# Self-contained: no libraries beyond the C++ runtime, linked statically.
arch_flags=${ARCH_FLAGS:--march=cascadelake -mtune=cascadelake}
${CXX:-g++} -std=c++20 -O3 -DNDEBUG $arch_flags -flto=auto -pthread -static \
  main.cpp links.cpp ingest.cpp aggregate.cpp output.cpp \
  -o "${1:-shopping-sessions}"
