#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-build-coverage}"
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}"
JOBS="${JOBS:-}"
CTEST_EXCLUDE_REGEX="${CTEST_EXCLUDE_REGEX:-hfile_chaos_kill}"
CMAKE_ARGS=()
LOCAL_PREFIX="${ROOT_DIR}/.conda-hfilesdk"
PLATFORM="$(uname -s)"
IS_LINUX=0
IS_MACOS=0

if [[ "${PLATFORM}" == "Linux" ]]; then
  IS_LINUX=1
elif [[ "${PLATFORM}" == "Darwin" ]]; then
  IS_MACOS=1
fi

detect_jobs() {
  if [[ "${IS_MACOS}" -eq 1 ]]; then
    sysctl -n hw.ncpu
    return 0
  fi
  if [[ "${IS_LINUX}" -eq 1 ]]; then
    if command -v nproc >/dev/null 2>&1; then
      nproc
      return 0
    fi
    if command -v getconf >/dev/null 2>&1; then
      getconf _NPROCESSORS_ONLN
      return 0
    fi
  fi
  if command -v getconf >/dev/null 2>&1; then
    getconf _NPROCESSORS_ONLN 2>/dev/null || true
    return 0
  fi
  echo 4
}

append_prefix_path() {
  local current="$1"
  local candidate="$2"
  if [[ -z "${candidate}" || ! -d "${candidate}" ]]; then
    echo "${current}"
    return 0
  fi
  if [[ -z "${current}" ]]; then
    echo "${candidate}"
    return 0
  fi
  case ";${current};" in
    *";${candidate};"*) echo "${current}" ;;
    *) echo "${current};${candidate}" ;;
  esac
}

has_cmake_arg_prefix() {
  local prefix="$1"
  shift
  for arg in "$@"; do
    if [[ "${arg}" == "${prefix}"* ]]; then
      return 0
    fi
  done
  return 1
}

JOBS="${JOBS:-$(detect_jobs)}"

PREFIX_PATH_VALUE="${CMAKE_PREFIX_PATH:-}"
PREFIX_PATH_VALUE="$(append_prefix_path "${PREFIX_PATH_VALUE}" "${LOCAL_PREFIX}")"
if [[ -n "${PREFIX_PATH_VALUE}" ]]; then
  CMAKE_ARGS+=("-DCMAKE_PREFIX_PATH=${PREFIX_PATH_VALUE}")
fi
if [[ -d "${LOCAL_PREFIX}/lib/cmake/Arrow" ]]; then
  CMAKE_ARGS+=("-DArrow_DIR=${Arrow_DIR:-${LOCAL_PREFIX}/lib/cmake/Arrow}")
fi

if [[ $# -gt 0 ]]; then
  CMAKE_ARGS+=("$@")
fi

if ! has_cmake_arg_prefix "-DHFILE_ENABLE_BENCHMARKS=" "${CMAKE_ARGS[@]}"; then
  CMAKE_ARGS+=("-DHFILE_ENABLE_BENCHMARKS=OFF")
fi

CONFIGURE_CMD=(
  cmake
  -S "${ROOT_DIR}"
  -B "${ROOT_DIR}/${BUILD_DIR}"
  -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
  -DHFILE_ENABLE_COVERAGE=ON
  "-DHFILE_COVERAGE_CTEST_ARGS=--output-on-failure;-E;${CTEST_EXCLUDE_REGEX}"
)
if ((${#CMAKE_ARGS[@]} > 0)); then
  CONFIGURE_CMD+=("${CMAKE_ARGS[@]}")
fi

echo "==> Configuring coverage build: ${BUILD_DIR}"
"${CONFIGURE_CMD[@]}"

echo "==> Building coverage targets"
cmake --build "${ROOT_DIR}/${BUILD_DIR}" -j"${JOBS}"

echo "==> Running coverage pipeline"
if [[ -n "${CTEST_EXCLUDE_REGEX}" ]]; then
  echo "==> Excluding timing-sensitive tests from coverage: ${CTEST_EXCLUDE_REGEX}"
fi
cmake --build "${ROOT_DIR}/${BUILD_DIR}" --target hfile_coverage_ci

echo ""
echo "Coverage summary: ${ROOT_DIR}/${BUILD_DIR}/coverage/summary.txt"
echo "Coverage HTML:    ${ROOT_DIR}/${BUILD_DIR}/coverage/html/index.html"
echo "CI artifacts:     ${ROOT_DIR}/${BUILD_DIR}/artifacts"
