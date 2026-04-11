# 脚本说明

## 当前脚本

- `build.sh`：本地标准构建入口，按 OS 类型探测并发数，并把项目内 `.conda-hfilesdk` 作为可选本地前缀
- `test.sh`：本地标准测试入口，执行 `cmake` 配置、构建，再运行 `ctest --output-on-failure`
- `coverage.sh`：覆盖率入口，按 OS 类型探测并发数，生成 `llvm-cov` 文本摘要与 HTML 报表；默认排除时间敏感的 `hfile_chaos_kill`
- `hfile-bulkload-perf-runner.sh`：集群环境下的性能对比包装入口，可选执行 `source` + `kinit`，然后调用 `tools/hfile-bulkload-perf` fat jar
- Windows 下同样使用上述 `.sh` 脚本；请在 MSYS2 `CLANG64` Shell 中通过 `bash scripts/*.sh` 执行

## 常用命令

Linux / macOS:

```bash
bash scripts/build.sh
bash scripts/test.sh
bash scripts/coverage.sh
bash scripts/hfile-bulkload-perf-runner.sh --help
```

Windows + MSYS2 `CLANG64`:

```bash
bash scripts/build.sh
bash scripts/test.sh
bash scripts/coverage.sh
bash scripts/hfile-bulkload-perf-runner.sh --help
```

## 常用参数

```bash
BUILD_DIR=build-debug CMAKE_BUILD_TYPE=Debug bash scripts/build.sh
BUILD_DIR=build-asan bash scripts/test.sh -DHFILE_ENABLE_ASAN=ON
bash scripts/test.sh -- -R test_arrow_converter
bash scripts/hfile-bulkload-perf-runner.sh --skip-login -- --help
```

`test.sh` 会把 `--` 之后的参数转交给 `ctest`；`build.sh` 和 `test.sh` 在 `--` 之前的参数会转交给 `cmake` 配置阶段。

## 平台说明

- `build.sh`、`test.sh`、`coverage.sh` 既可直接用于 macOS/Linux，也可在 Windows + MSYS2 `CLANG64` Shell 中直接执行
- `build.sh`、`test.sh`、`coverage.sh` 现在都按 OS 类型选择并发数探测逻辑，而不是仅靠 `sysctl`/`nproc` 是否存在来判断平台
- `build.sh`、`test.sh`、`coverage.sh` 中的项目内 `.conda-hfilesdk` 也只是可选本地前缀，不是 Linux/macOS 的必需路径；脚本会优先尊重外部传入的 `CMAKE_PREFIX_PATH` / `Arrow_DIR`
- `build.sh` 会在配置前检查 `cmake`、`clang/clang++`；`test.sh` 额外检查 `ctest`；`coverage.sh` 额外检查 `llvm-cov`、`llvm-profdata`，并在 macOS 下兼容 `xcrun --find`
- `hfile-bulkload-perf-runner.sh` 只负责环境准备与参数透传，性能矩阵、三轮统计与双实现调用均在 jar 内完成
- Windows 当前只维护 `MSYS2 + clang/clang++` 这一条路径；建议先进入 `CLANG64` Shell，再在该环境中执行 `bash scripts/*.sh`
- Windows + MSYS2 场景下，脚本本身不会额外拉起 Bash 包装层；请确保 `bash`、`cmake`、`clang/clang++`、Java、Maven 与 Arrow 依赖在当前 Shell 中可见
- `coverage.sh` 会在配置前检查 `cmake`、`clang/clang++`、`llvm-cov`、`llvm-profdata`，并在 Windows + MSYS2 场景下打印更明确的前置提示
