#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-build-coverage}"
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

echo "==> Configuring coverage build: ${BUILD_DIR}"
cmake -S "${ROOT_DIR}" -B "${ROOT_DIR}/${BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
  -DHFILE_ENABLE_COVERAGE=ON \
  "${CMAKE_ARGS[@]}"

echo "==> Building coverage targets"
cmake --build "${ROOT_DIR}/${BUILD_DIR}" -j"${JOBS}"

echo "==> Running coverage pipeline"
cmake --build "${ROOT_DIR}/${BUILD_DIR}" --target hfile_coverage_ci

echo ""
echo "Coverage summary: ${ROOT_DIR}/${BUILD_DIR}/coverage/summary.txt"
echo "Coverage HTML:    ${ROOT_DIR}/${BUILD_DIR}/coverage/html/index.html"
echo "CI artifacts:     ${ROOT_DIR}/${BUILD_DIR}/artifacts"
