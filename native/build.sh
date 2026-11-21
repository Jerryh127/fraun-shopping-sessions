#!/bin/sh
set -eu
# The official PyArrow wheel includes matching C++ headers and shared libraries.
arrow_dir=${ARROW_DIR:-$(python3 -c 'import os, pyarrow; print(os.path.dirname(pyarrow.__file__))')}
arch_flags=${ARCH_FLAGS:--march=cascadelake -mtune=cascadelake}
${CXX:-g++} -std=c++20 -O3 -DNDEBUG $arch_flags -flto=auto -pthread \
  -I"$arrow_dir/include" main.cpp links.cpp ingest.cpp aggregate.cpp output.cpp \
  -L"$arrow_dir" -Wl,-rpath,"$arrow_dir" -l:libparquet.so.2000 -l:libarrow.so.2000 \
  -o "${1:-shopping-sessions}"
