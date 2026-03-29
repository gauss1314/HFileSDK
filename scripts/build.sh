#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-build}"
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}"
JOBS="${JOBS:-}"
CMAKE_ARGS=()

if [[ -z "${JOBS}" ]]; then
  if command -v sysctl >/dev/null 2>&1; then
    JOBS="$(sysctl -n hw.ncpu)"
  elif command -v nproc >/dev/null 2>&1; then
    JOBS="$(nproc)"
  else
    JOBS=4
  fi
fi

DEFAULT_PREFIX="${ROOT_DIR}/.conda-hfilesdk"
if [[ -d "${DEFAULT_PREFIX}" ]]; then
  CMAKE_ARGS+=("-DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH:-${DEFAULT_PREFIX}}")
  if [[ -d "${DEFAULT_PREFIX}/lib/cmake/Arrow" ]]; then
    CMAKE_ARGS+=("-DArrow_DIR=${Arrow_DIR:-${DEFAULT_PREFIX}/lib/cmake/Arrow}")
  fi
fi

if [[ $# -gt 0 ]]; then
  CMAKE_ARGS+=("$@")
fi

CONFIGURE_CMD=(
  cmake
  -S "${ROOT_DIR}"
  -B "${ROOT_DIR}/${BUILD_DIR}"
  -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
)
if ((${#CMAKE_ARGS[@]} > 0)); then
  CONFIGURE_CMD+=("${CMAKE_ARGS[@]}")
fi

echo "==> Configuring build: ${BUILD_DIR}"
"${CONFIGURE_CMD[@]}"

echo "==> Building targets"
cmake --build "${ROOT_DIR}/${BUILD_DIR}" -j"${JOBS}"

echo ""
echo "Build directory: ${ROOT_DIR}/${BUILD_DIR}"
