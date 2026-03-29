# 脚本说明

## 当前脚本

- `build.sh`：本地标准构建入口，自动探测并发数，并在存在 `.conda-hfilesdk` 时自动补 `CMAKE_PREFIX_PATH` 与 `Arrow_DIR`
- `test.sh`：本地标准测试入口，执行 `cmake` 配置、构建，再运行 `ctest --output-on-failure`
- `coverage.sh`：覆盖率入口，生成 `llvm-cov` 文本摘要与 HTML 报表
- `bench-runner.sh`：基准与可选 HBase bulk load 流水线，默认更适合 Linux 环境

## 常用命令

```bash
bash scripts/build.sh
bash scripts/test.sh
bash scripts/coverage.sh
```

## 常用参数

```bash
BUILD_DIR=build-debug CMAKE_BUILD_TYPE=Debug bash scripts/build.sh
BUILD_DIR=build-asan bash scripts/test.sh -DHFILE_ENABLE_ASAN=ON
bash scripts/test.sh -- -R test_arrow_converter
bash scripts/build.sh -DHFILE_ENABLE_BENCHMARKS=ON
```

`test.sh` 会把 `--` 之后的参数转交给 `ctest`；`build.sh` 和 `test.sh` 在 `--` 之前的参数会转交给 `cmake` 配置阶段。

## 平台说明

- `build.sh`、`test.sh`、`coverage.sh` 面向 macOS/Linux
- `bench-runner.sh` 依赖 `taskset`、`/proc/sys/vm/drop_caches` 等 Linux 特性，不建议作为 macOS 默认入口
