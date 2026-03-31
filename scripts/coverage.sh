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
  # Windows / MSYS2: NUMBER_OF_PROCESSORS is always set by the OS
  if [[ -n "${NUMBER_OF_PROCESSORS:-}" ]]; then
    echo "${NUMBER_OF_PROCESSORS}"
    return 0
  fi
  if command -v getconf >/dev/null 2>&1; then
    local n
    n="$(getconf _NPROCESSORS_ONLN 2>/dev/null)"
    if [[ -n "${n}" && "${n}" -gt 0 ]] 2>/dev/null; then
      echo "${n}"
      return 0
    fi
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

# Prepend `candidate` to the front of a semicolon-separated prefix list.
# Used to ensure MSYS2 CLANG64 packages take priority over /usr packages.
prepend_prefix_path() {
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
    *) echo "${candidate};${current}" ;;
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

require_llvm_tool() {
  local tool_name="$1"
  if command -v "${tool_name}" >/dev/null 2>&1; then
    return 0
  fi
  if [[ "${IS_MACOS}" -eq 1 ]] && command -v xcrun >/dev/null 2>&1; then
    if xcrun --find "${tool_name}" >/dev/null 2>&1; then
      return 0
    fi
  fi
  echo "Missing required command: ${tool_name}" >&2
  echo "Install llvm tools in the active environment so CMake can find ${tool_name}." >&2
  exit 1
}

print_windows_msys2_hints() {
  if [[ "${PLATFORM}" == MINGW* || "${PLATFORM}" == MSYS* || "${PLATFORM}" == CYGWIN* ]]; then
    echo "==> Windows/MSYS2 mode detected"
    echo "    MSYSTEM=${MSYSTEM:-<unset>}"
    if [[ -z "${MSYSTEM:-}" ]]; then
      echo "    Hint: start from an MSYS2 Clang shell, or set MSYSTEM=CLANG64 before running coverage.bat" >&2
    fi
  fi
}

preflight_coverage_env() {
  print_windows_msys2_hints
  require_command cmake "Install CMake or ensure it is visible in the MSYS2 shell PATH."
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
  require_llvm_tool llvm-cov
  require_llvm_tool llvm-profdata
  if [[ -z "${Arrow_DIR:-}" && ! -d "${LOCAL_PREFIX}/lib/cmake/Arrow" && -z "${CMAKE_PREFIX_PATH:-}" ]]; then
    echo "Warning: Arrow_DIR/CMAKE_PREFIX_PATH not set and no local prefix found at ${LOCAL_PREFIX}." >&2
    echo "         CMake may still succeed if Arrow is installed in a default prefix visible to this environment." >&2
  fi
}

JOBS="${JOBS:-$(detect_jobs)}"

# On MSYS2, detect the active environment prefix early so all find_package()
# calls pick up the correct ABI-compatible packages (e.g. GTest, protobuf).
# MSYS2 sets MSYSTEM_PREFIX automatically: /clang64, /mingw64, etc.
# We prepend it so it takes priority over /usr packages which use a different runtime.
MSYS2_ACTIVE_PREFIX=""
if [[ "${PLATFORM}" == MINGW* || "${PLATFORM}" == MSYS* || "${PLATFORM}" == CYGWIN* ]]; then
  MSYS2_ACTIVE_PREFIX="${MSYSTEM_PREFIX:-/clang64}"
fi

PREFIX_PATH_VALUE="${CMAKE_PREFIX_PATH:-}"
# Prepend MSYS2 environment prefix FIRST so CLANG64 packages win over /usr packages.
if [[ -n "${MSYS2_ACTIVE_PREFIX}" ]]; then
  PREFIX_PATH_VALUE="$(prepend_prefix_path "${PREFIX_PATH_VALUE}" "${MSYS2_ACTIVE_PREFIX}")"
fi
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
  CMAKE_ARGS+=("$@")
fi

if ! has_cmake_arg_prefix "-DHFILE_ENABLE_BENCHMARKS=" "${CMAKE_ARGS[@]}"; then
  CMAKE_ARGS+=("-DHFILE_ENABLE_BENCHMARKS=OFF")
fi

preflight_coverage_env

# On Windows / MSYS2 force Ninja generator and Clang compiler.
IS_WINDOWS_MSYS2=0
if [[ "${PLATFORM}" == MINGW* || "${PLATFORM}" == MSYS* || "${PLATFORM}" == CYGWIN* ]]; then
  IS_WINDOWS_MSYS2=1
fi

CONFIGURE_CMD=(
  cmake
  -S "${ROOT_DIR}"
  -B "${ROOT_DIR}/${BUILD_DIR}"
  -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
  -DHFILE_ENABLE_COVERAGE=ON
  "-DHFILE_COVERAGE_CTEST_ARGS=--output-on-failure;-E;${CTEST_EXCLUDE_REGEX}"
)
if [[ "${IS_WINDOWS_MSYS2}" -eq 1 ]]; then
  if ! has_cmake_arg_prefix "-G" "${CMAKE_ARGS[@]+"${CMAKE_ARGS[@]}"}"; then
    CONFIGURE_CMD+=(-G Ninja)
  fi
  if ! has_cmake_arg_prefix "-DCMAKE_CXX_COMPILER=" "${CMAKE_ARGS[@]+"${CMAKE_ARGS[@]}"}"; then
    CONFIGURE_CMD+=("-DCMAKE_CXX_COMPILER=clang++")
  fi
  if ! has_cmake_arg_prefix "-DCMAKE_C_COMPILER=" "${CMAKE_ARGS[@]+"${CMAKE_ARGS[@]}"}"; then
    CONFIGURE_CMD+=("-DCMAKE_C_COMPILER=clang")
  fi
fi
if ((${#CMAKE_ARGS[@]} > 0)); then
  CONFIGURE_CMD+=("${CMAKE_ARGS[@]}")
fi

echo "==> Configuring coverage build: ${BUILD_DIR}"
"${CONFIGURE_CMD[@]}"

echo "==> Building coverage targets"
# Guard against empty JOBS (safety net for unexpected detect_jobs failures)
if [[ -z "${JOBS:-}" || ! "${JOBS}" =~ ^[0-9]+$ ]]; then JOBS=4; fi
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
