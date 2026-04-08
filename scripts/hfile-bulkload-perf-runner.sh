#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAVA_BIN="${JAVA_BIN:-java}"
ENV_SCRIPT="${ENV_SCRIPT:-}"
KERBEROS_PRINCIPAL="${KERBEROS_PRINCIPAL:-}"
KERBEROS_KEYTAB="${KERBEROS_KEYTAB:-}"
PERF_JAR_DEFAULT="${ROOT_DIR}/tools/hfile-bulkload-perf/target/hfile-bulkload-perf-1.0.0.jar"
PERF_JAR="${PERF_JAR:-${PERF_JAR_DEFAULT}}"
NATIVE_LIB="${NATIVE_LIB:-}"
TABLE_NAME="${TABLE_NAME:-}"
WORK_DIR="${WORK_DIR:-/tmp/hfilesdk-bulkload-perf}"
BULKLOAD_DIR="${BULKLOAD_DIR:-/tmp/hbase_bulkload}"
HDFS_STAGING_DIR="${HDFS_STAGING_DIR:-}"
HBASE_BIN="${HBASE_BIN:-hbase}"
HDFS_BIN="${HDFS_BIN:-hdfs}"
SKIP_LOGIN=0
declare -a PERF_ARGS=()

usage() {
  cat <<EOF
用法:
  bash scripts/hfile-bulkload-perf-runner.sh [脚本参数] [-- 透传给 perf jar 的参数]

脚本参数:
  --env-script PATH         集群环境脚本，例如 /opt/client/bigdata_env
  --principal NAME          Kerberos principal
  --keytab PATH             Kerberos keytab
  --skip-login              跳过 source env 与 kinit
  --perf-jar PATH           hfile-bulkload-perf fat jar 路径
  --native-lib PATH         libhfilesdk 动态库路径
  --table NAME              HBase 表名
  --work-dir PATH           本地工作目录
  --bulkload-dir PATH       本地 HFile staging 目录
  --hdfs-staging-dir PATH   HDFS staging 根目录
  --hbase-bin BIN           hbase 命令路径
  --hdfs-bin BIN            hdfs 命令路径
  --help                    显示帮助

示例:
  bash scripts/hfile-bulkload-perf-runner.sh \
    --env-script /opt/client/bigdata_env \
    --principal ossuser \
    --keytab /opt/client/keytab/ossuser.keytab \
    --native-lib ./release/libhfilesdk.so \
    --table tdr_signal_stor_20550 \
    --hdfs-staging-dir /hbase/staging/job_20550 \
    -- \
    --cf cf \
    --arrow-file-count 8 \
    --target-size-mb 256 \
    --parallelism 4
EOF
}

require_command() {
  local cmd="$1"
  if command -v "${cmd}" >/dev/null 2>&1; then
    return 0
  fi
  echo "Missing required command: ${cmd}" >&2
  exit 1
}

require_file() {
  local path="$1"
  local name="$2"
  if [[ -f "${path}" ]]; then
    return 0
  fi
  echo "${name} not found: ${path}" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-script) ENV_SCRIPT="$2"; shift 2 ;;
    --principal) KERBEROS_PRINCIPAL="$2"; shift 2 ;;
    --keytab) KERBEROS_KEYTAB="$2"; shift 2 ;;
    --skip-login) SKIP_LOGIN=1; shift ;;
    --perf-jar) PERF_JAR="$2"; shift 2 ;;
    --native-lib) NATIVE_LIB="$2"; shift 2 ;;
    --table) TABLE_NAME="$2"; shift 2 ;;
    --work-dir) WORK_DIR="$2"; shift 2 ;;
    --bulkload-dir) BULKLOAD_DIR="$2"; shift 2 ;;
    --hdfs-staging-dir) HDFS_STAGING_DIR="$2"; shift 2 ;;
    --hbase-bin) HBASE_BIN="$2"; shift 2 ;;
    --hdfs-bin) HDFS_BIN="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    --) shift; PERF_ARGS+=("$@"); break ;;
    *) PERF_ARGS+=("$1"); shift ;;
  esac
done

if [[ "${SKIP_LOGIN}" -eq 0 ]]; then
  if [[ -z "${ENV_SCRIPT}" ]]; then
    echo "--env-script is required unless --skip-login is set" >&2
    exit 1
  fi
  if [[ -z "${KERBEROS_PRINCIPAL}" || -z "${KERBEROS_KEYTAB}" ]]; then
    echo "--principal and --keytab are required unless --skip-login is set" >&2
    exit 1
  fi
fi

if [[ -z "${NATIVE_LIB}" ]]; then
  echo "--native-lib is required" >&2
  exit 1
fi
if [[ -z "${TABLE_NAME}" ]]; then
  echo "--table is required" >&2
  exit 1
fi
if [[ -z "${HDFS_STAGING_DIR}" ]]; then
  echo "--hdfs-staging-dir is required" >&2
  exit 1
fi

require_command "${JAVA_BIN}"
require_file "${PERF_JAR}" "perf jar"
require_file "${NATIVE_LIB}" "native library"

declare -a CMD=(
  "${JAVA_BIN}"
  "-jar"
  "${PERF_JAR}"
  "--native-lib" "${NATIVE_LIB}"
  "--table" "${TABLE_NAME}"
  "--work-dir" "${WORK_DIR}"
  "--bulkload-dir" "${BULKLOAD_DIR}"
  "--hdfs-staging-dir" "${HDFS_STAGING_DIR}"
  "--hbase-bin" "${HBASE_BIN}"
  "--hdfs-bin" "${HDFS_BIN}"
)

if [[ "${#PERF_ARGS[@]}" -gt 0 ]]; then
  CMD+=("${PERF_ARGS[@]}")
fi

if [[ "${SKIP_LOGIN}" -eq 0 ]]; then
  require_file "${ENV_SCRIPT}" "env script"
  require_file "${KERBEROS_KEYTAB}" "keytab"
  source "${ENV_SCRIPT}"
  require_command kinit
  kinit -kt "${KERBEROS_KEYTAB}" "${KERBEROS_PRINCIPAL}"
fi

echo "▶ Running hfile-bulkload-perf"
printf '  %q' "${CMD[@]}"
printf '\n'
"${CMD[@]}"
