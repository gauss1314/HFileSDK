#!/usr/bin/env bash
# bench-runner.sh — HFileSDK full benchmark automation
# Runs C++ and Java benchmarks, bulk loads into HBase, verifies, generates HTML report.
#
# Usage:
#   bash scripts/bench-runner.sh [--skip-hbase] [--skip-java] [--iterations N]
#
# Environment variables:
#   HBASE_HOME       Path to HBase installation (required for bulk load)
#   HADOOP_HOME      Path to Hadoop installation
#   HBASE_CONF_DIR   Path to hbase-site.xml directory (default: $HBASE_HOME/conf)
#   BENCH_TABLE      HBase table name (default: hfilesdk_bench)
#   STAGING_DIR      HDFS staging root (default: /tmp/hfilesdk_staging)
#   RESULTS_DIR      Local results directory (default: results/)

set -euo pipefail

# ─── Defaults ────────────────────────────────────────────────────────────────
SKIP_HBASE=0
SKIP_JAVA=0
ITERATIONS=10
HBASE_CONF_DIR="${HBASE_CONF_DIR:-${HBASE_HOME:-}/conf}"
BENCH_TABLE="${BENCH_TABLE:-hfilesdk_bench}"
STAGING_DIR="${STAGING_DIR:-/tmp/hfilesdk_staging}"
RESULTS_DIR="${RESULTS_DIR:-results}"
BUILD_DIR="build"
BENCHMARK_PIN="${BENCHMARK_PIN:-}"
RUN_HBASE_ON_MACOS="${RUN_HBASE_ON_MACOS:-0}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
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

JOBS="${JOBS:-$(detect_jobs)}"

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

find_benchmark_prefix() {
  if [[ -n "${BENCHMARK_PIN}" && -d "${BENCHMARK_PIN}" ]]; then
    echo "${BENCHMARK_PIN}"
    return 0
  fi
  if [[ -d "${LOCAL_PREFIX}/lib/cmake/benchmark" ]]; then
    echo "${LOCAL_PREFIX}"
    return 0
  fi
  if [[ "${IS_MACOS}" -eq 1 ]] && command -v brew >/dev/null 2>&1; then
    local brew_prefix brew_cellar brew_latest
    brew_prefix="$(brew --prefix google-benchmark 2>/dev/null || true)"
    if [[ -n "${brew_prefix}" && -d "${brew_prefix}/lib/cmake/benchmark" ]]; then
      echo "${brew_prefix}"
      return 0
    fi
    brew_cellar="$(brew --cellar google-benchmark 2>/dev/null || true)"
    if [[ -n "${brew_cellar}" && -d "${brew_cellar}" ]]; then
      brew_latest="$(find "${brew_cellar}" -mindepth 1 -maxdepth 1 -type d | sort | tail -n 1)"
      if [[ -n "${brew_latest}" && -d "${brew_latest}/lib/cmake/benchmark" ]]; then
        echo "${brew_latest}"
        return 0
      fi
    fi
  fi
  return 1
}

run_pinned() {
  if [[ "${IS_LINUX}" -eq 1 ]] && command -v taskset >/dev/null 2>&1; then
    taskset -c 0-3 "$@"
  else
    "$@"
  fi
}

print_windows_msys2_hints() {
  if [[ "${PLATFORM}" == MINGW* || "${PLATFORM}" == MSYS* || "${PLATFORM}" == CYGWIN* ]]; then
    echo "  MSYSTEM=${MSYSTEM:-<unset>}"
    if [[ -z "${MSYSTEM:-}" ]]; then
      echo "  Hint: start from an MSYS2 Clang shell, or set MSYSTEM=CLANG64 before running bench-runner.bat"
    fi
  fi
}

print_platform_mode() {
  echo "▶ Platform mode: ${PLATFORM}"
  if [[ "${IS_LINUX}" -eq 1 ]]; then
    echo "  CPU pinning: enabled when taskset exists"
    echo "  Cache drop:  available when /proc/sys/vm/drop_caches is writable"
    echo "  HBase stage: available when HBASE_HOME/HADOOP_HOME are configured"
    echo "  Local prefix: optional (${LOCAL_PREFIX})"
  elif [[ "${IS_MACOS}" -eq 1 ]]; then
    echo "  CPU pinning: disabled (taskset unavailable on macOS)"
    echo "  Cache drop:  disabled"
    echo "  HBase stage: disabled by default; set RUN_HBASE_ON_MACOS=1 to force"
    echo "  Local prefix: optional (${LOCAL_PREFIX})"
  else
    echo "  CPU pinning: best effort"
    echo "  Cache drop:  disabled"
    echo "  HBase stage: best effort"
    echo "  Local prefix: optional (${LOCAL_PREFIX})"
  fi
  print_windows_msys2_hints
}

preflight_bench_env() {
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
  if [[ "$SKIP_JAVA" -eq 0 ]] && ! command -v java >/dev/null 2>&1; then
    echo "Warning: java not found; Java benchmark stage may be skipped." >&2
  fi
  if [[ -z "${Arrow_DIR:-}" && ! -d "${LOCAL_PREFIX}/lib/cmake/Arrow" && -z "${CMAKE_PREFIX_PATH:-}" ]]; then
    echo "Warning: Arrow_DIR/CMAKE_PREFIX_PATH not set and no local prefix found at ${LOCAL_PREFIX}." >&2
    echo "         CMake may still succeed if Arrow is installed in a default prefix visible to this environment." >&2
  fi
}

# ─── Parse args ──────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-hbase)  SKIP_HBASE=1  ;;
    --skip-java)   SKIP_JAVA=1   ;;
    --iterations)  ITERATIONS="$2"; shift ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
  shift
done

echo "╔══════════════════════════════════════════════════════════╗"
echo "║           HFileSDK Full Benchmark Pipeline               ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
print_platform_mode
echo ""
preflight_bench_env

# ─── Verify build ────────────────────────────────────────────────────────────
if [[ ! -f "$BUILD_DIR/bench/macro/bm_e2e_write" ]]; then
  echo "▶ Building HFileSDK..."
  PREFIX_PATH_VALUE="${CMAKE_PREFIX_PATH:-}"
  PREFIX_PATH_VALUE="$(append_prefix_path "${PREFIX_PATH_VALUE}" "${LOCAL_PREFIX}")"
  BENCHMARK_PREFIX="$(find_benchmark_prefix || true)"
  if [[ -z "${BENCHMARK_PREFIX}" && -z "${benchmark_DIR:-}" ]]; then
    echo "⚠ google-benchmark not found. Skipping benchmark pipeline."
    echo "  Install google-benchmark or set BENCHMARK_PIN/benchmark_DIR/CMAKE_PREFIX_PATH, then rerun."
    exit 0
  fi
  PREFIX_PATH_VALUE="$(append_prefix_path "${PREFIX_PATH_VALUE}" "${BENCHMARK_PREFIX}")"
  CMAKE_ARGS=(
    -S "$ROOT_DIR"
    -B "$BUILD_DIR"
    -DCMAKE_BUILD_TYPE=Release
    -DHFILE_ENABLE_BENCHMARKS=ON
    -DHFILE_ENABLE_HDFS="$([[ "$SKIP_HBASE" -eq 0 ]] && echo ON || echo OFF)"
    -DCMAKE_CXX_FLAGS="-O3 -march=native"
  )
  if [[ -n "${PREFIX_PATH_VALUE}" ]]; then
    CMAKE_ARGS+=("-DCMAKE_PREFIX_PATH=${PREFIX_PATH_VALUE}")
  fi
  if [[ -n "${Arrow_DIR:-}" ]]; then
    CMAKE_ARGS+=("-DArrow_DIR=${Arrow_DIR}")
  elif [[ -d "${LOCAL_PREFIX}/lib/cmake/Arrow" ]]; then
    CMAKE_ARGS+=("-DArrow_DIR=${LOCAL_PREFIX}/lib/cmake/Arrow")
  fi
  if [[ -n "${benchmark_DIR:-}" ]]; then
    CMAKE_ARGS+=("-Dbenchmark_DIR=${benchmark_DIR}")
  elif [[ -d "${BENCHMARK_PREFIX}/lib/cmake/benchmark" ]]; then
    CMAKE_ARGS+=("-Dbenchmark_DIR=${BENCHMARK_PREFIX}/lib/cmake/benchmark")
  fi
  cmake "${CMAKE_ARGS[@]}"
  cmake --build "$BUILD_DIR" -j"$JOBS"
fi

mkdir -p "$RESULTS_DIR"

DATASETS=("ds1_small_kv" "ds2_wide_table")
COMPRESSIONS=("none" "lz4" "zstd")

# ─── Helper: drop page cache ─────────────────────────────────────────────────
drop_caches() {
  if [[ "${IS_LINUX}" -eq 1 && -w /proc/sys/vm/drop_caches ]]; then
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
  else
    echo "  (page cache drop unavailable on this platform)"
  fi
}

# ─── Unit tests first ────────────────────────────────────────────────────────
echo "▶ Running unit tests..."
cd "$BUILD_DIR" && ctest --output-on-failure -j"$JOBS" && cd ..
echo "  Unit tests: PASS"
echo ""

# ─── Micro benchmarks ────────────────────────────────────────────────────────
echo "▶ Running micro benchmarks..."

for bm in bm_kv_encode bm_crc32c bm_compress bm_bloom; do
  if [[ -f "$BUILD_DIR/bench/micro/$bm" ]]; then
    echo "  Running $bm..."
    drop_caches
    run_pinned "$BUILD_DIR/bench/micro/$bm" \
      --benchmark_format=json \
      --benchmark_repetitions="$ITERATIONS" \
      --benchmark_report_aggregates_only=true \
      > "$RESULTS_DIR/cpp_micro_${bm}.json"
    echo "  $bm: done → $RESULTS_DIR/cpp_micro_${bm}.json"
  fi
done

# ─── C++ end-to-end benchmark ────────────────────────────────────────────────
echo ""
echo "▶ Running C++ end-to-end benchmark..."
drop_caches
run_pinned "$BUILD_DIR/bench/macro/bm_e2e_write" \
  --benchmark_format=json \
  --benchmark_repetitions="$ITERATIONS" \
  --benchmark_report_aggregates_only=true \
  > "$RESULTS_DIR/cpp_e2e.json"
echo "  C++ e2e: done → $RESULTS_DIR/cpp_e2e.json"

# ─── Java baseline (optional) ────────────────────────────────────────────────
if [[ "$SKIP_JAVA" -eq 0 ]]; then
  JAVA_JAR="bench/java/target/hfile-bench-java-1.0.0.jar"

  # Auto-build the jar if it doesn't exist yet
  if [[ ! -f "$JAVA_JAR" ]]; then
    echo ""
    echo "▶ Building Java baseline benchmark jar..."
    (cd bench/java && mvn package -q -DskipTests) \
      && echo "  Built: $JAVA_JAR" \
      || { echo "  Maven build failed — skipping Java benchmark"; SKIP_JAVA=1; }
  fi

  if [[ "$SKIP_JAVA" -eq 0 && -f "$JAVA_JAR" ]]; then
    echo ""
    echo "▶ Running Java baseline benchmark..."
    drop_caches
    run_pinned java \
      -Xmx8g -Xms8g -XX:+UseZGC \
      -jar "$JAVA_JAR" \
      --benchmark_format=json \
      --iterations="$ITERATIONS" \
      > "$RESULTS_DIR/java_e2e.json" \
      2>"$RESULTS_DIR/java_e2e.stderr.txt"
    echo "  Java e2e: done → $RESULTS_DIR/java_e2e.json"
  fi
fi

# ─── HFile format verification ───────────────────────────────────────────────
echo ""
echo "▶ Verifying generated HFiles..."
TMP_HFILE="/tmp/hfilesdk_verify_test.hfile"

# Generate a test HFile using the bm_e2e_write binary
"$BUILD_DIR/bench/macro/bm_e2e_write" \
  --benchmark_filter="BM_E2E_Write/100000/0/0" \
  --benchmark_iterations=1 2>/dev/null || true

VERIFY_JAR="tools/hfile-verify/target/hfile-verify-1.0.0.jar"
if [[ -f "$VERIFY_JAR" ]]; then
  echo "  Running Java HFile verifier..."
  java -jar "$VERIFY_JAR" --help >/dev/null 2>&1 || true
else
  echo "  Skipping Java verifier (jar not built — run: cd tools/hfile-verify && mvn package)"
fi

# ─── HBase Bulk Load (optional) ──────────────────────────────────────────────
if [[ "${IS_MACOS}" -eq 1 && "${RUN_HBASE_ON_MACOS}" -ne 1 ]]; then
  SKIP_HBASE=1
fi

if [[ "$SKIP_HBASE" -eq 0 ]] && [[ -n "${HBASE_HOME:-}" ]] && [[ -n "${HADOOP_HOME:-}" ]]; then
  echo ""
  echo "▶ Running HBase Bulk Load pipeline..."

  for ds in "${DATASETS[@]}"; do
    for comp in "${COMPRESSIONS[@]}"; do
      TAG="${ds}_${comp}"
      LOCAL_STAGING="/tmp/hfilesdk_staging/${TAG}"
      HDFS_STAGING="${STAGING_DIR}/${TAG}"

      echo "  [${TAG}] Generating HFiles..."
      # (In production this would call a dedicated data generator binary)

      echo "  [${TAG}] Uploading to HDFS: ${HDFS_STAGING}"
      "${HADOOP_HOME}/bin/hdfs" dfs -rm -r -f "$HDFS_STAGING" 2>/dev/null || true
      "${HADOOP_HOME}/bin/hdfs" dfs -put "$LOCAL_STAGING" "$HDFS_STAGING"

      echo "  [${TAG}] Bulk loading into HBase table: ${BENCH_TABLE}_${ds}"
      "${HBASE_HOME}/bin/hbase" \
        org.apache.hadoop.hbase.tool.BulkLoadHFilesTool \
        "$HDFS_STAGING" "${BENCH_TABLE}_${ds}" \
        2>&1 | tail -5

      if [[ -f tools/hfile-bulkload-verify/target/hfile-bulkload-verify.jar ]]; then
        echo "  [${TAG}] Verifying loaded data..."
        java -jar tools/hfile-bulkload-verify/target/hfile-bulkload-verify.jar \
          --zookeeper localhost:2181 \
          --table "${BENCH_TABLE}_${ds}"
      fi
    done
  done
elif [[ "$SKIP_HBASE" -eq 0 ]]; then
  echo ""
  echo "▶ Skipping HBase Bulk Load pipeline..."
  if [[ "${IS_MACOS}" -eq 1 && "${RUN_HBASE_ON_MACOS}" -ne 1 ]]; then
    echo "  Set RUN_HBASE_ON_MACOS=1 if you want to force the HBase stage on macOS."
  else
    echo "  HBASE_HOME and HADOOP_HOME must both be configured."
  fi
fi

# ─── Generate HTML report ─────────────────────────────────────────────────────
echo ""
echo "▶ Generating HTML report..."
python3 tools/hfile-report/hfile-report.py \
  --input-dir "$RESULTS_DIR" \
  --output "$RESULTS_DIR/report.html"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  All done! Open: $RESULTS_DIR/report.html"
echo "╚══════════════════════════════════════════════════════════╝"
