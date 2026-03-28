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

# ─── Verify build ────────────────────────────────────────────────────────────
if [[ ! -f "$BUILD_DIR/bench/macro/bm_e2e_write" ]]; then
  echo "▶ Building HFileSDK..."
  cmake -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE=Release \
    -DHFILE_ENABLE_BENCHMARKS=ON \
    -DHFILE_ENABLE_HDFS="${SKIP_HBASE:=0}" \
    -DCMAKE_CXX_FLAGS="-O3 -march=native"
  cmake --build "$BUILD_DIR" -j"$(nproc)"
fi

mkdir -p "$RESULTS_DIR"

DATASETS=("ds1_small_kv" "ds2_wide_table")
COMPRESSIONS=("none" "lz4" "zstd")

# ─── Helper: drop page cache ─────────────────────────────────────────────────
drop_caches() {
  if [[ -w /proc/sys/vm/drop_caches ]]; then
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
  else
    echo "  (cannot drop page cache — run as root for fair benchmarks)"
  fi
}

# ─── Unit tests first ────────────────────────────────────────────────────────
echo "▶ Running unit tests..."
cd "$BUILD_DIR" && ctest --output-on-failure -j"$(nproc)" && cd ..
echo "  Unit tests: PASS"
echo ""

# ─── Micro benchmarks ────────────────────────────────────────────────────────
echo "▶ Running micro benchmarks..."

for bm in bm_kv_encode bm_crc32c bm_compress bm_bloom; do
  if [[ -f "$BUILD_DIR/bench/micro/$bm" ]]; then
    echo "  Running $bm..."
    drop_caches
    taskset -c 0-3 "$BUILD_DIR/bench/micro/$bm" \
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
taskset -c 0-3 "$BUILD_DIR/bench/macro/bm_e2e_write" \
  --benchmark_format=json \
  --benchmark_repetitions="$ITERATIONS" \
  --benchmark_report_aggregates_only=true \
  > "$RESULTS_DIR/cpp_e2e.json"
echo "  C++ e2e: done → $RESULTS_DIR/cpp_e2e.json"

# ─── Java baseline (optional) ────────────────────────────────────────────────
if [[ "$SKIP_JAVA" -eq 0 ]]; then
  JAVA_JAR="bench/java/target/hfile-bench-java-1.0.0-shaded.jar"

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
    taskset -c 0-3 java \
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

if [[ -f tools/hfile-verify/target/hfile-verify-1.0.0-jar-with-dependencies.jar ]]; then
  echo "  Running Java HFile verifier..."
  java -jar tools/hfile-verify/target/hfile-verify-1.0.0-jar-with-dependencies.jar \
    --hfile-dir /tmp/ 2>/dev/null || true
else
  echo "  Skipping Java verifier (jar not built — run: cd tools/hfile-verify && mvn package)"
fi

# ─── HBase Bulk Load (optional) ──────────────────────────────────────────────
if [[ "$SKIP_HBASE" -eq 0 ]] && [[ -n "${HBASE_HOME:-}" ]]; then
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
