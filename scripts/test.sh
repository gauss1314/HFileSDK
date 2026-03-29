#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-build}"
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-Release}"
JOBS="${JOBS:-}"
CMAKE_ARGS=()
CTEST_ARGS=()
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

require_command() {
  local cmd="$1"
  local hint="$2"
  if command -v "${cmd}" >/dev/null 2>&1; then
    return 0
  fi
  echo "Missing required command: ${cmd}" >&2
  if [[ -n "${hint}" ]]; then
    echo "${hint}" >&2
  fi
  exit 1
}

print_windows_msys2_hints() {
  if [[ "${PLATFORM}" == MINGW* || "${PLATFORM}" == MSYS* || "${PLATFORM}" == CYGWIN* ]]; then
    echo "==> Windows/MSYS2 mode detected"
    echo "    MSYSTEM=${MSYSTEM:-<unset>}"
    if [[ -z "${MSYSTEM:-}" ]]; then
      echo "    Hint: start from an MSYS2 Clang shell, or set MSYSTEM=CLANG64 before running test.bat" >&2
    fi
  fi
}

preflight_test_env() {
  print_windows_msys2_hints
  require_command cmake "Install CMake or ensure it is visible in the active shell PATH."
  require_command ctest "Install CMake with ctest support or ensure ctest is visible in the active shell PATH."
  if [[ -z "${CMAKE_CXX_COMPILER:-}" ]]; then
    if command -v clang++ >/dev/null 2>&1; then
      :
    elif command -v clang >/dev/null 2>&1; then
      :
    else
      echo "Missing required compiler: clang/clang++" >&2
      echo "Hint: install the MSYS2 Clang toolchain and launch the script from the matching shell." >&2
      exit 1
    fi
  fi
  if [[ -z "${Arrow_DIR:-}" && ! -d "${LOCAL_PREFIX}/lib/cmake/Arrow" && -z "${CMAKE_PREFIX_PATH:-}" ]]; then
    echo "Warning: Arrow_DIR/CMAKE_PREFIX_PATH not set and no local prefix found at ${LOCAL_PREFIX}." >&2
    echo "         CMake may still succeed if Arrow is installed in a default prefix visible to this environment." >&2
  fi
}

JOBS="${JOBS:-$(detect_jobs)}"

PREFIX_PATH_VALUE="${CMAKE_PREFIX_PATH:-}"
PREFIX_PATH_VALUE="$(append_prefix_path "${PREFIX_PATH_VALUE}" "${LOCAL_PREFIX}")"
if [[ -n "${PREFIX_PATH_VALUE}" ]]; then
  CMAKE_ARGS+=("-DCMAKE_PREFIX_PATH=${PREFIX_PATH_VALUE}")
fi
if [[ -n "${Arrow_DIR:-}" ]]; then
  CMAKE_ARGS+=("-DArrow_DIR=${Arrow_DIR}")
elif [[ -d "${LOCAL_PREFIX}/lib/cmake/Arrow" ]]; then
  CMAKE_ARGS+=("-DArrow_DIR=${LOCAL_PREFIX}/lib/cmake/Arrow")
fi

if [[ $# -gt 0 ]]; then
  if [[ "$1" == "--" ]]; then
    shift
    CTEST_ARGS+=("$@")
  else
    while [[ $# -gt 0 ]]; do
      if [[ "$1" == "--" ]]; then
        shift
        CTEST_ARGS+=("$@")
        break
      fi
      CMAKE_ARGS+=("$1")
      shift
    done
  fi
fi

if ! has_cmake_arg_prefix "-DHFILE_ENABLE_BENCHMARKS=" "${CMAKE_ARGS[@]}"; then
  CMAKE_ARGS+=("-DHFILE_ENABLE_BENCHMARKS=OFF")
fi

preflight_test_env

CONFIGURE_CMD=(
  cmake
  -S "${ROOT_DIR}"
  -B "${ROOT_DIR}/${BUILD_DIR}"
  -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
)
if ((${#CMAKE_ARGS[@]} > 0)); then
  CONFIGURE_CMD+=("${CMAKE_ARGS[@]}")
fi

CTEST_CMD=(
  ctest
  --test-dir "${ROOT_DIR}/${BUILD_DIR}"
  --output-on-failure
)
if ((${#CTEST_ARGS[@]} > 0)); then
  CTEST_CMD+=("${CTEST_ARGS[@]}")
fi

echo "==> Configuring test build: ${BUILD_DIR}"
"${CONFIGURE_CMD[@]}"

echo "==> Building test targets"
cmake --build "${ROOT_DIR}/${BUILD_DIR}" -j"${JOBS}"

echo "==> Running ctest"
"${CTEST_CMD[@]}"
