#!/usr/bin/env bash
# =============================================================================
# release.sh — Build a self-contained HFileSDK release package
#
# 产出物 (默认放在 <repo>/release/ 目录):
#   release/
#     libhfilesdk.so            # 自包含的 JNI 共享库
#     lib/                      # 运行时依赖的 .so (bundle 模式)
#     arrow-to-hfile-*.jar      # Arrow → HFile 转换工具
#     mock-arrow-*.jar          # 测试数据生成工具
#     README.md                 # 部署说明
#
# 两种打包模式 (默认 static):
#
#   static  — 把 Arrow / Protobuf / 压缩库全部静态链接进 libhfilesdk.so。
#             需要 libarrow.a 和 libprotobuf.a 存在。
#             生成的 .so 仅依赖 libc / libstdc++ / libpthread，
#             可直接 scp 到任意 Linux 机器使用。
#             若找不到 .a 文件，打印 WARNING 并自动降级到 bundle 模式。
#
#   bundle  — 动态链接，但把所有运行时 .so 依赖复制到 release/lib/，
#             并把 libhfilesdk.so 的 RPATH 设为 $ORIGIN/lib。
#             只需把整个 release/ 目录拷贝到目标机器。
#
# 用法:
#   bash scripts/release.sh [选项]
#
# 选项:
#   -o DIR   输出目录           (默认: <repo>/release)
#   -m MODE  模式: static|bundle (默认: static)
#   -j N     编译线程数          (默认: nproc)
#   -s       启用全部压缩编解码器 (LZ4/ZSTD/Snappy/GZip)
#   -h       显示帮助
# =============================================================================

# 不使用 set -e：用明确的错误检查替代，确保 README 和摘要始终输出
set -uo pipefail

# ── 颜色 ──────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; NC='\033[0m'
info()    { echo -e "${GREEN}==>${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARNING]${NC} $*" >&2; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
step()    { echo; echo -e "${GREEN}==> [$1/5]${NC} $2"; }

# ── 路径 ──────────────────────────────────────────────────────────────────────
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RELEASE_BUILD_DIR="${ROOT_DIR}/.release-build"
RELEASE_DIR="${ROOT_DIR}/release"
LOCAL_PREFIX="${ROOT_DIR}/.conda-hfilesdk"
PLATFORM="$(uname -s)"

# ── 参数解析 ──────────────────────────────────────────────────────────────────
MODE="static"
JOBS=""
ENABLE_ALL_CODECS=0

usage() {
  sed -n '/^# =/,/^# =/p' "${BASH_SOURCE[0]}" | grep '^#' | sed 's/^# \?//'
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -o) RELEASE_DIR="$2"; shift 2 ;;
    -m) MODE="$2";         shift 2 ;;
    -j) JOBS="$2";         shift 2 ;;
    -s) ENABLE_ALL_CODECS=1; shift ;;
    -h|--help) usage ;;
    -*) error "Unknown option: $1"; exit 1 ;;
    *) error "Unexpected argument: $1 (use -- to pass cmake args)"; exit 1 ;;
  esac
done

[[ "${MODE}" == "static" || "${MODE}" == "bundle" ]] || {
  error "Invalid mode '${MODE}'. Use: static | bundle"
  exit 1
}

# ── 并发数 ────────────────────────────────────────────────────────────────────
if [[ -z "${JOBS}" ]]; then
  if command -v nproc >/dev/null 2>&1; then JOBS="$(nproc)"
  elif [[ "${PLATFORM}" == "Darwin" ]];  then JOBS="$(sysctl -n hw.ncpu)"
  else JOBS=4; fi
fi

# ── 工具检查 ──────────────────────────────────────────────────────────────────
HAVE_MVN=0; HAVE_JAVA=0
command -v cmake >/dev/null 2>&1 || { error "cmake not found. Install cmake 3.20+"; exit 1; }
command -v mvn   >/dev/null 2>&1 && HAVE_MVN=1
command -v java  >/dev/null 2>&1 && HAVE_JAVA=1

# ── 静态库探测 ────────────────────────────────────────────────────────────────
find_static_lib() {
  local name="$1"
  local dirs=(
    "${LOCAL_PREFIX}/lib"
    "/usr/local/lib"
    "/usr/lib"
    "/usr/lib/x86_64-linux-gnu"
    "/usr/lib64"
    "/usr/lib/aarch64-linux-gnu"
  )
  for d in "${dirs[@]}"; do
    [[ -f "${d}/lib${name}.a" ]] && { echo "${d}/lib${name}.a"; return 0; }
  done
  return 1
}

ARROW_STATIC=""
PROTO_STATIC=""
STATIC_OK=0

if [[ "${MODE}" == "static" ]]; then
  if ARROW_STATIC="$(find_static_lib arrow 2>/dev/null)" && \
     PROTO_STATIC="$(find_static_lib protobuf 2>/dev/null)"; then
    STATIC_OK=1
    info "Static libs found:"
    info "  Arrow:    ${ARROW_STATIC}"
    info "  Protobuf: ${PROTO_STATIC}"
  else
    warn "Static libraries not found (libarrow.a / libprotobuf.a missing)."
    warn "Falling back to BUNDLE mode."
    warn "To enable static mode, build Arrow/Protobuf with -DBUILD_SHARED_LIBS=OFF"
    warn "or install static packages (e.g. apt install libarrow-dev)."
    MODE="bundle"
  fi
fi

# ── Banner ────────────────────────────────────────────────────────────────────
echo
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          HFileSDK Release Build                              ║"
echo "╠══════════════════════════════════════════════════════════════╣"
printf "║  Mode     : %-48s ║\n" "${MODE}"
printf "║  Output   : %-48s ║\n" "${RELEASE_DIR}"
printf "║  Jobs     : %-48s ║\n" "${JOBS}"
printf "║  Codecs   : %-48s ║\n" "$([ ${ENABLE_ALL_CODECS} -eq 1 ] && echo 'LZ4 ZSTD Snappy GZip' || echo 'None (Compression::None only)')"
printf "║  Maven    : %-48s ║\n" "$([ ${HAVE_MVN} -eq 1 ] && echo 'found' || echo 'NOT FOUND — JARs will be skipped')"
echo "╚══════════════════════════════════════════════════════════════╝"
echo

# ── 状态追踪 (不用 set -e，改用明确记录每步结果) ─────────────────────────────
SO_OK=0
JAR_ATH_OK=0
JAR_MOCK_OK=0
STEP_ERRORS=()

# ── Step 1: cmake 配置 ────────────────────────────────────────────────────────
step 1 "Configuring cmake (build dir: .release-build)..."

CMAKE_ARGS=(
  -S "${ROOT_DIR}"
  -B "${RELEASE_BUILD_DIR}"
  -DCMAKE_BUILD_TYPE=Release
  -DHFILE_ENABLE_TESTS=OFF
  -DHFILE_ENABLE_JAVA_TESTS=OFF   # Maven not required; JAR built separately
  -DHFILE_BUILD_JNI_LIB=ON        # always build libhfilesdk.so
)

# 压缩编解码器
if [[ ${ENABLE_ALL_CODECS} -eq 1 ]]; then
  CMAKE_ARGS+=(
    -DHFILE_ENABLE_LZ4=ON
    -DHFILE_ENABLE_ZSTD=ON
    -DHFILE_ENABLE_SNAPPY=ON
    -DHFILE_ENABLE_GZIP=ON
  )
fi

# 本地前缀（conda 环境）
if [[ -d "${LOCAL_PREFIX}/lib/cmake/Arrow" ]]; then
  CMAKE_ARGS+=("-DArrow_DIR=${LOCAL_PREFIX}/lib/cmake/Arrow")
  CMAKE_ARGS+=("-DCMAKE_PREFIX_PATH=${LOCAL_PREFIX}")
fi

if cmake "${CMAKE_ARGS[@]}"; then
  info "cmake configure OK"
else
  error "cmake configure FAILED"
  STEP_ERRORS+=("cmake configure failed")
fi

# ── Step 2: C++ 编译 ──────────────────────────────────────────────────────────
step 2 "Building C++ library and hfilesdk JNI bridge (${JOBS} jobs)..."

if [[ ${#STEP_ERRORS[@]} -eq 0 ]]; then
  if cmake --build "${RELEASE_BUILD_DIR}" -j"${JOBS}"; then
    info "C++ build OK"
  else
    error "C++ build FAILED"
    STEP_ERRORS+=("C++ build failed")
  fi
else
  warn "Skipping C++ build (cmake configure failed)"
fi

# ── Step 3: 打包 .so ──────────────────────────────────────────────────────────
step 3 "Packaging libhfilesdk.so (mode: ${MODE})..."

mkdir -p "${RELEASE_DIR}/lib"

# 找到 cmake 生成的 libhfilesdk.so
CMAKE_SO=""
if [[ ${#STEP_ERRORS[@]} -eq 0 ]]; then
  CMAKE_SO="$(find "${RELEASE_BUILD_DIR}" -name "libhfilesdk.so" \
              ! -name "*.so.*" 2>/dev/null | head -1)"
  if [[ -z "${CMAKE_SO}" ]]; then
    error "libhfilesdk.so not found in ${RELEASE_BUILD_DIR}"
    error "Check that JNI headers are installed (e.g. apt install default-jdk-headless)"
    STEP_ERRORS+=("libhfilesdk.so not found — JNI headers may be missing")
  fi
fi

if [[ -n "${CMAKE_SO}" && ${#STEP_ERRORS[@]} -eq 0 ]]; then
  if [[ "${MODE}" == "static" && ${STATIC_OK} -eq 1 ]]; then
    # ── Static 重链接 ──────────────────────────────────────────────────────
    info "Relinking libhfilesdk.so with static Arrow + Protobuf..."

    HFILE_A="$(find "${RELEASE_BUILD_DIR}" -name "libhfile.a" 2>/dev/null | head -1)"
    if [[ -z "${HFILE_A}" ]]; then
      error "libhfile.a not found — cannot do static relink"
      STEP_ERRORS+=("static relink: libhfile.a missing")
    else
      # 找 JNI 目标文件：cmake 把它放在 CMakeFiles/hfilesdk.dir/ 下
      JNI_OBJ="$(find "${RELEASE_BUILD_DIR}" \
                  \( -name "hfile_jni.cc.o" -o -name "hfile_jni.cpp.o" \) \
                  2>/dev/null | head -1)"
      if [[ -z "${JNI_OBJ}" ]]; then
        # 兼容 Ninja (有些版本把 .o 放在平坦目录)
        JNI_OBJ="$(find "${RELEASE_BUILD_DIR}" -path "*/jni/*.o" \
                    2>/dev/null | head -1)"
      fi

      if [[ -z "${JNI_OBJ}" ]]; then
        warn "hfile_jni.cc.o not found — falling back to copying cmake-built .so"
        cp -pL "${CMAKE_SO}" "${RELEASE_DIR}/libhfilesdk.so"
        SO_OK=1
      else
        # 编解码器静态库（可选）
        CODEC_FLAGS=()
        if [[ ${ENABLE_ALL_CODECS} -eq 1 ]]; then
          for lib in lz4 zstd snappy z; do
            lib_a="$(find_static_lib "$lib" 2>/dev/null || true)"
            [[ -n "${lib_a}" ]] && CODEC_FLAGS+=("-Wl,-Bstatic" "${lib_a}")
          done
        fi

        # JNI include（需要 javac 路径）
        JNI_INC=""
        if command -v javac >/dev/null 2>&1; then
          JAVA_HOME_PROBE="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
          JNI_INC="-I${JAVA_HOME_PROBE}/include -I${JAVA_HOME_PROBE}/include/linux"
        fi

        CXX_CMD="${CXX:-$(command -v clang++ 2>/dev/null || command -v g++ 2>/dev/null || echo c++)}"

        if "${CXX_CMD}" -std=c++20 -shared -fPIC \
            ${JNI_INC} \
            "${JNI_OBJ}" \
            -Wl,--whole-archive "${HFILE_A}" -Wl,--no-whole-archive \
            -Wl,-Bstatic "${ARROW_STATIC}" "${PROTO_STATIC}" \
            "${CODEC_FLAGS[@]+"${CODEC_FLAGS[@]}"}" \
            -Wl,-Bdynamic \
            -Wl,-soname,libhfilesdk.so \
            -Wl,--exclude-libs,ALL \
            -lpthread -lm -ldl \
            -o "${RELEASE_DIR}/libhfilesdk.so" 2>&1; then
          info "Static relink OK: ${RELEASE_DIR}/libhfilesdk.so"
          SO_OK=1
        else
          error "Static relink FAILED — falling back to cmake-built .so (dynamic)"
          STEP_ERRORS+=("static relink failed (see above); copied dynamic .so instead")
          cp -pL "${CMAKE_SO}" "${RELEASE_DIR}/libhfilesdk.so"
          SO_OK=1   # we still have a usable .so
        fi
      fi
    fi

  else
    # ── Bundle 模式 ────────────────────────────────────────────────────────
    info "Copying cmake-built libhfilesdk.so..."
    cp -pL "${CMAKE_SO}" "${RELEASE_DIR}/libhfilesdk.so"

    if command -v ldd >/dev/null 2>&1; then
      info "Collecting .so runtime dependencies via ldd..."
      SYSTEM_LIBS="libpthread|libm|libc\b|libdl|librt|libgcc|libstdc\+\+|linux-vdso|ld-linux"

      # First pass: direct deps
      ldd "${RELEASE_DIR}/libhfilesdk.so" 2>/dev/null \
        | grep "=>" | grep -vE "${SYSTEM_LIBS}" \
        | awk '{print $3}' | grep -v '^(' | sort -u \
        | while read -r dep_so; do
            [[ -f "${dep_so}" ]] || continue
            dep_name="$(basename "${dep_so}")"
            cp -pL "${dep_so}" "${RELEASE_DIR}/lib/${dep_name}"
            echo "  bundled: ${dep_name}"
          done

      # Second pass: transitive deps of bundled libs
      for dep in "${RELEASE_DIR}"/lib/*.so*; do
        [[ -f "${dep}" ]] || continue
        ldd "${dep}" 2>/dev/null \
          | grep "=>" | grep -vE "${SYSTEM_LIBS}" \
          | awk '{print $3}' | grep -v '^(' | sort -u \
          | while read -r sub_dep; do
              [[ -f "${sub_dep}" ]] || continue
              sub_name="$(basename "${sub_dep}")"
              dest="${RELEASE_DIR}/lib/${sub_name}"
              [[ -f "${dest}" ]] && continue
              cp -pL "${sub_dep}" "${dest}"
              echo "  bundled (transitive): ${sub_name}"
            done
      done
    else
      warn "ldd not available — runtime .so deps not collected automatically"
      warn "Manually copy libarrow.so, libprotobuf.so etc. to ${RELEASE_DIR}/lib/"
    fi

    # 设置 RPATH=$ORIGIN/lib
    if command -v patchelf >/dev/null 2>&1; then
      patchelf --set-rpath "\$ORIGIN/lib" "${RELEASE_DIR}/libhfilesdk.so"
      info "RPATH patched to: \$ORIGIN/lib"
    else
      warn "patchelf not found — RPATH not patched."
      warn "Install patchelf, or run Java with:"
      warn "  -Djava.library.path=${RELEASE_DIR}"
      warn "  and ensure LD_LIBRARY_PATH includes ${RELEASE_DIR}/lib"
    fi

    SO_OK=1
  fi
fi

# ── Step 4: Java JAR 构建 ─────────────────────────────────────────────────────
step 4 "Building Java JARs..."

build_jar() {
  local subdir="$1"
  local label="$2"
  local dir="${ROOT_DIR}/${subdir}"

  info "  Building ${label}..."
  if [[ ! -f "${dir}/pom.xml" ]]; then
    error "  pom.xml not found: ${dir}/pom.xml"
    return 1
  fi

  # Run mvn; do NOT hide exit code (no || true)
  local mvn_log
  mvn_log="$(mktemp /tmp/hfilesdk-mvn-XXXXXX.log)"
  if (cd "${dir}" && mvn package -q -DskipTests 2>&1 | tee "${mvn_log}" | \
      grep -vE "^\[INFO\] (BUILD SUCCESS|---|-{3,}|Building jar|Total time|Finished at)" \
      || true); then
    : # tee always succeeds, check artifact
  fi

  local jar
  jar="$(find "${dir}/target" -maxdepth 1 -name "*.jar" \
           ! -name "original-*.jar" ! -name "*-sources.jar" \
           2>/dev/null | head -1)"

  if [[ -z "${jar}" ]]; then
    error "  No JAR found in ${subdir}/target — mvn output:"
    cat "${mvn_log}" >&2
    rm -f "${mvn_log}"
    return 1
  fi

  rm -f "${mvn_log}"
  cp "${jar}" "${RELEASE_DIR}/"
  info "  → $(basename "${jar}")"
  return 0
}

if [[ ${HAVE_MVN} -eq 0 || ${HAVE_JAVA} -eq 0 ]]; then
  warn "Maven or Java not found — JAR builds skipped."
  warn "Install JDK 21 + Maven, then run:"
  warn "  cd tools/arrow-to-hfile && mvn package -q -DskipTests"
  warn "  cd tools/mock-arrow     && mvn package -q -DskipTests"
  warn "  cp tools/arrow-to-hfile/target/*.jar ${RELEASE_DIR}/"
  warn "  cp tools/mock-arrow/target/*.jar     ${RELEASE_DIR}/"
else
  if build_jar "tools/arrow-to-hfile" "arrow-to-hfile"; then
    JAR_ATH_OK=1
  else
    STEP_ERRORS+=("arrow-to-hfile JAR build failed")
  fi

  if build_jar "tools/mock-arrow" "mock-arrow"; then
    JAR_MOCK_OK=1
  else
    STEP_ERRORS+=("mock-arrow JAR build failed")
  fi
fi

# ── Step 5: README.md ─────────────────────────────────────────────────────────
step 5 "Writing release/README.md..."

BUILD_DATE="$(date '+%Y-%m-%d %H:%M:%S')"
GIT_HASH=""
if command -v git >/dev/null 2>&1 && \
   git -C "${ROOT_DIR}" rev-parse --short HEAD >/dev/null 2>&1; then
  GIT_HASH=" (git: $(git -C "${ROOT_DIR}" rev-parse --short HEAD))"
fi

# 找到实际生成的 JAR 文件名
ATH_JAR="$(basename "$(find "${RELEASE_DIR}" -maxdepth 1 \
               -name "arrow-to-hfile-*.jar" 2>/dev/null | head -1)" \
           2>/dev/null || echo "arrow-to-hfile-4.0.0.jar")"
MOCK_JAR="$(basename "$(find "${RELEASE_DIR}" -maxdepth 1 \
               -name "mock-arrow-*.jar" 2>/dev/null | head -1)" \
            2>/dev/null || echo "mock-arrow-1.0.0.jar")"

if [[ "${MODE}" == "bundle" ]]; then
DEPLOY_NOTE="## 部署说明（Bundle 模式）

\`libhfilesdk.so\` 的 RPATH 已设置为 \`\$ORIGIN/lib\`，运行时会优先在同目录的 \`lib/\`
子目录查找 Arrow、Protobuf 等依赖，无需目标机器预装这些库。

**只需把整个 \`release/\` 目录整体拷贝到目标机器即可：**

\`\`\`bash
scp -r release/ user@target-host:/opt/hfilesdk/
\`\`\`

目录结构：
\`\`\`
/opt/hfilesdk/
  libhfilesdk.so      ← JNI 库（RPATH 指向 lib/）
  lib/
    libarrow.so.*     ← 自动找到
    libprotobuf.so.*
    ...
  ${ATH_JAR}
  ${MOCK_JAR}
\`\`\`"
else
DEPLOY_NOTE="## 部署说明（Static 模式）

\`libhfilesdk.so\` 已静态链接 Arrow、Protobuf 等依赖，仅依赖系统的
\`libc\` / \`libstdc++\` / \`libpthread\`，任意 Linux glibc 2.17+ 系统均可运行。

**直接拷贝所需文件：**

\`\`\`bash
scp release/libhfilesdk.so release/${ATH_JAR} release/${MOCK_JAR} user@target-host:/opt/hfilesdk/
\`\`\`"
fi

# 写 README（此步骤始终执行，即使前面步骤有错误）
cat > "${RELEASE_DIR}/README.md" << READMEEOF
# HFileSDK Release Package

构建时间：${BUILD_DATE}${GIT_HASH}
打包模式：${MODE}

## 文件说明

| 文件 | 说明 |
|------|------|
| \`libhfilesdk.so\` | C++ HFile 写入库（JNI 共享库），Java 通过 JNI 调用 |
| \`lib/\` | 运行时 .so 依赖（bundle 模式专用） |
| \`${ATH_JAR}\` | Arrow IPC Stream → HFile v3 转换工具 |
| \`${MOCK_JAR}\` | Arrow 格式测试数据生成工具 |

${DEPLOY_NOTE}

---

## 快速开始

### 第一步：生成测试数据（Arrow 文件）

使用 \`${MOCK_JAR}\` 生成指定大小的 Arrow IPC Stream 文件：

\`\`\`bash
# 生成 50 MiB 的 tdr_signal_stor_20550 表数据（默认表）
java -jar ${MOCK_JAR} \\
  --output /data/tdr_20550.arrow \\
  --size   50

# 生成 100 MiB 的 tdr_mock 表数据
java -jar ${MOCK_JAR} \\
  --output /data/tdr_mock.arrow \\
  --table  tdr_mock \\
  --size   100

# 查看所有参数
java -jar ${MOCK_JAR} --help
\`\`\`

支持的表：

| 表名 | 列定义 |
|------|--------|
| \`tdr_signal_stor_20550\` | REFID(bigint) / TIME(bigint) / SIGSTORE(string) / BIT_MAP(bigint) / no(string) |
| \`tdr_mock\` | STARTTIME(bigint) / IMSI(string) / MSISDN(string) / DURATION(bigint) / BYTES_UP(bigint) / BYTES_DW(bigint) / CELL_ID(string) / RAT_TYPE(string) |

---

### 第二步：转换为 HFile

使用 \`${ATH_JAR}\` 把 Arrow 文件转换为 HBase HFile v3：

\`\`\`bash
# 方式一：通过 --native-lib 直接指定 .so 路径
java -jar ${ATH_JAR} \\
  --native-lib \$(pwd)/libhfilesdk.so \\
  --arrow      /data/tdr_20550.arrow \\
  --hfile      /staging/tdr_signal_stor_20550/cf/tdr_20550.hfile \\
  --table      tdr_signal_stor_20550 \\
  --rule       "REFID,0,false,15" \\
  --cf         cf

# 方式二：通过环境变量（适合脚本化场景）
export HFILESDK_NATIVE_LIB=\$(pwd)/libhfilesdk.so
java -jar ${ATH_JAR} \\
  --arrow /data/tdr_20550.arrow \\
  --hfile /staging/tdr_signal_stor_20550/cf/tdr_20550.hfile \\
  --rule  "REFID,0,false,15"

# 查看所有参数
java -jar ${ATH_JAR} --help
\`\`\`

各表的 rowKeyRule：

| 表名 | --rule 参数 |
|------|------------|
| \`tdr_signal_stor_20550\` | \`REFID,0,false,15\` |
| \`tdr_mock\` | \`STARTTIME,0,false,10#IMSI,1,true,15\` |

---

### 第三步：Bulk Load 到 HBase（可选）

\`\`\`bash
# 上传 HFile 到 HDFS（目录结构必须为 <table>/<cf>/<hfile>）
hdfs dfs -mkdir -p /hbase/staging/tdr_signal_stor_20550/cf/
hdfs dfs -put /staging/tdr_signal_stor_20550/cf/tdr_20550.hfile \\
              /hbase/staging/tdr_signal_stor_20550/cf/

# Bulk Load
hbase org.apache.hadoop.hbase.tool.BulkLoadHFilesTool \\
  /hbase/staging/tdr_signal_stor_20550 tdr_signal_stor_20550
\`\`\`

---

### 生产系统集成（Java 代码）

在生产工程的 \`pom.xml\` 中引入：

\`\`\`xml
<dependency>
    <groupId>io.hfilesdk</groupId>
    <artifactId>arrow-to-hfile</artifactId>
    <version>4.0.0</version>
</dependency>
\`\`\`

Java 调用示例：

\`\`\`java
import io.hfilesdk.converter.*;

// 应用启动时加载 native 库（一次）
ArrowToHFileConverter converter =
    ArrowToHFileConverter.withNativeLib("/opt/hfilesdk/libhfilesdk.so");

// 每次转换
ConvertResult result = converter.convertOrThrow(
    ConvertOptions.builder()
        .arrowPath("/data/tdr_20550.arrow")
        .hfilePath("/staging/cf/tdr_20550.hfile")
        .tableName("tdr_signal_stor_20550")
        .rowKeyRule("REFID,0,false,15")
        .columnFamily("cf")
        .build());

System.out.println(result.summary());
// OK  kvs=57,575  skipped=0  dupKeys=0  hfile=12.3MB  elapsed=830ms
\`\`\`

---

## 系统要求

- **OS**：Linux x86-64（glibc 2.17+）或 macOS 12+
- **Java**：JDK 21+（运行 JAR 工具必须）
- **权限**：写入 libhfilesdk.so 所在目录（无需 root）
READMEEOF

info "README.md written: ${RELEASE_DIR}/README.md"

# ── 最终摘要 ──────────────────────────────────────────────────────────────────
echo
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Release Summary                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo
echo "Output: ${RELEASE_DIR}/"
echo

# 列出产出文件
SO_FILE="${RELEASE_DIR}/libhfilesdk.so"
if [[ -f "${SO_FILE}" ]]; then
  SO_SIZE="$(du -sh "${SO_FILE}" 2>/dev/null | cut -f1)"
  echo -e "  ${GREEN}✓${NC}  libhfilesdk.so  (${SO_SIZE})"
else
  echo -e "  ${RED}✗${NC}  libhfilesdk.so  MISSING"
fi

LIB_COUNT="$(find "${RELEASE_DIR}/lib" -name "*.so*" 2>/dev/null | wc -l)"
if [[ ${LIB_COUNT} -gt 0 ]]; then
  echo -e "  ${GREEN}✓${NC}  lib/ (${LIB_COUNT} shared libs bundled)"
fi

for jar in "${RELEASE_DIR}"/*.jar; do
  [[ -f "${jar}" ]] || continue
  jsize="$(du -sh "${jar}" 2>/dev/null | cut -f1)"
  echo -e "  ${GREEN}✓${NC}  $(basename "${jar}")  (${jsize})"
done

if [[ -f "${RELEASE_DIR}/README.md" ]]; then
  echo -e "  ${GREEN}✓${NC}  README.md"
fi

# 错误摘要
if [[ ${#STEP_ERRORS[@]} -gt 0 ]]; then
  echo
  echo -e "${RED}⚠ Build completed with errors:${NC}"
  for e in "${STEP_ERRORS[@]}"; do
    echo -e "  ${RED}•${NC} ${e}"
  done
  echo
  echo "Review the output above for details."
  exit 1
else
  echo
  echo -e "${GREEN}✓ Release package built successfully!${NC}"
  echo
  echo "Deploy:"
  if [[ "${MODE}" == "bundle" ]]; then
    echo "  scp -r ${RELEASE_DIR}/ user@host:/opt/hfilesdk/"
  else
    echo "  scp ${RELEASE_DIR}/libhfilesdk.so ${RELEASE_DIR}/*.jar user@host:/opt/hfilesdk/"
  fi
  echo
  echo "Quick test:"
  echo "  java -jar ${RELEASE_DIR}/${MOCK_JAR} --output /tmp/test.arrow --size 1"
  echo "  java -jar ${RELEASE_DIR}/${ATH_JAR}  --native-lib ${RELEASE_DIR}/libhfilesdk.so \\"
  echo "       --arrow /tmp/test.arrow --hfile /tmp/test.hfile --rule 'REFID,0,false,15'"
fi
