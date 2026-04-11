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

preflight_test_env

# On Windows / MSYS2 the default cmake generator is usually "MSYS Makefiles"
# or even "Visual Studio" which does not work with Clang.  Force Ninja and
# explicitly set the compiler so cmake always picks up the MSYS2 Clang build.
IS_WINDOWS_MSYS2=0
if [[ "${PLATFORM}" == MINGW* || "${PLATFORM}" == MSYS* || "${PLATFORM}" == CYGWIN* ]]; then
  IS_WINDOWS_MSYS2=1
fi

CONFIGURE_CMD=(
  cmake
  -S "${ROOT_DIR}"
  -B "${ROOT_DIR}/${BUILD_DIR}"
  -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
)
# Inject Ninja generator + Clang compiler on Windows / MSYS2 unless the
# caller has already specified a generator or compiler via CMAKE_ARGS.
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

  # ── Force GTest to the CLANG64 prefix via Windows-native path ─────────────
  # cmake.exe is a native Windows process and CANNOT resolve MSYS2 POSIX paths
  # like /clang64.  MSYSTEM_PREFIX=/clang64 is unusable by cmake directly.
  # Instead, use cygpath to convert to a Windows path (e.g. D:/msys64/clang64)
  # and pass it as -DGTest_DIR on the cmake command line.  Command-line -D args
  # have the HIGHEST priority in cmake — they override CMakeCache.txt entries
  # and the set(... FORCE) calls in CMakeLists.txt.
  # This is the belt-and-suspenders fix: even if CMakeLists.txt gets confused,
  # the -D argument guarantees cmake finds the right GTest.
  if ! has_cmake_arg_prefix "-DGTest_DIR=" "${CMAKE_ARGS[@]+"${CMAKE_ARGS[@]}"}"; then
    _MSYS2_WIN_PREFIX=""
    if command -v cygpath >/dev/null 2>&1; then
      # cygpath -m converts POSIX → Windows with forward slashes (cmake-friendly)
      _MSYS2_WIN_PREFIX="$(cygpath -m "${MSYS2_ACTIVE_PREFIX:-/clang64}")"
    fi
    if [[ -n "${_MSYS2_WIN_PREFIX}" &&           -f "${_MSYS2_WIN_PREFIX}/lib/cmake/GTest/GTestConfig.cmake" ]]; then
      CONFIGURE_CMD+=("-DGTest_DIR=${_MSYS2_WIN_PREFIX}/lib/cmake/GTest")
      echo "==> MSYS2: GTest_DIR → ${_MSYS2_WIN_PREFIX}/lib/cmake/GTest"
    else
      echo "==> MSYS2 WARNING: GTestConfig.cmake not found at"            "${_MSYS2_WIN_PREFIX}/lib/cmake/GTest" >&2
      echo "    Install it with: pacman -S mingw-w64-clang-x86_64-gtest" >&2
    fi
  fi
fi
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


# ── MSYS2: detect stale CMakeCache with wrong GTest prefix ───────────────────
# cmake caches GTest_DIR, GTEST_LIBRARY, GTEST_MAIN_LIBRARY etc. in
# CMakeCache.txt.  If any of these point at /usr/ (MSYS runtime, incompatible
# ABI) we must wipe the cache before configure.
# Paths may use forward OR back slashes in CMakeCache.txt — match both.
if [[ "${IS_WINDOWS_MSYS2}" -eq 1 ]]; then
  CACHE_FILE="${ROOT_DIR}/${BUILD_DIR}/CMakeCache.txt"
  if [[ -f "${CACHE_FILE}" ]]; then
    CACHED_GTEST=$(sed 's|\\|/|g' "${CACHE_FILE}" 2>/dev/null       | grep -iE "^(GTest_DIR|GTEST_INCLUDE_DIR|GTEST_LIBRARY|GTEST_MAIN_LIBRARY).*="       | grep -i "/usr/" | head -1)
    if [[ -n "${CACHED_GTEST}" ]]; then
      echo "==> MSYS2: stale GTest cache detected (wrong ABI — points at /usr):"
      echo "    ${CACHED_GTEST}"
      echo "==> Wiping cmake cache for a clean configure..."
      rm -f  "${CACHE_FILE}"
      rm -rf "${ROOT_DIR}/${BUILD_DIR}/CMakeFiles"
      echo "==> Cache wiped."
    fi
  fi
fi

echo "==> Configuring test build: ${BUILD_DIR}"
"${CONFIGURE_CMD[@]}"

echo "==> Building test targets"
# Guard against empty JOBS (safety net for unexpected detect_jobs failures)
if [[ -z "${JOBS:-}" || ! "${JOBS}" =~ ^[0-9]+$ ]]; then JOBS=4; fi
cmake --build "${ROOT_DIR}/${BUILD_DIR}" -j"${JOBS}"

echo "==> Running ctest"
"${CTEST_CMD[@]}"
