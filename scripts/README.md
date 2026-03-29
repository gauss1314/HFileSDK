# 脚本说明

## 当前脚本

- `build.sh`：本地标准构建入口，按 OS 类型探测并发数，并把项目内 `.conda-hfilesdk` 作为可选本地前缀
- `test.sh`：本地标准测试入口，执行 `cmake` 配置、构建，再运行 `ctest --output-on-failure`
- `coverage.sh`：覆盖率入口，按 OS 类型探测并发数，生成 `llvm-cov` 文本摘要与 HTML 报表；默认排除时间敏感的 `hfile_chaos_kill`
- `bench-runner.sh`：基准与可选 HBase bulk load 流水线；会按 Linux/macOS 自动切换模式，在无 `taskset` 时退化为普通执行，找不到 `google-benchmark` 时给出提示并跳过
- `build.bat` / `test.bat` / `coverage.bat` / `bench-runner.bat`：Windows 批处理入口，适用于已安装 MSYS2 并使用其中 Bash/Clang 的环境
- `_run_msys2_script.bat`：Windows 包装脚本公共入口，负责定位 MSYS2 `bash.exe` 并转调同名 `.sh`

## 常用命令

```bash
bash scripts/build.sh
bash scripts/test.sh
bash scripts/coverage.sh
bash scripts/bench-runner.sh --skip-hbase --skip-java --iterations 1
```

Windows + MSYS2:

```bat
scripts\build.bat
scripts\test.bat
scripts\coverage.bat
scripts\bench-runner.bat --skip-hbase --skip-java --iterations 1
```

## 常用参数

```bash
BUILD_DIR=build-debug CMAKE_BUILD_TYPE=Debug bash scripts/build.sh
BUILD_DIR=build-asan bash scripts/test.sh -DHFILE_ENABLE_ASAN=ON
bash scripts/test.sh -- -R test_arrow_converter
bash scripts/build.sh -DHFILE_ENABLE_BENCHMARKS=ON
RUN_HBASE_ON_MACOS=1 bash scripts/bench-runner.sh --iterations 3
```

`test.sh` 会把 `--` 之后的参数转交给 `ctest`；`build.sh` 和 `test.sh` 在 `--` 之前的参数会转交给 `cmake` 配置阶段。
`build.sh`、`test.sh`、`coverage.sh` 默认会显式设置 `-DHFILE_ENABLE_BENCHMARKS=OFF`，避免复用旧的 CMakeCache 时误触 benchmark 依赖；如需开启 benchmark，请显式传入 `-DHFILE_ENABLE_BENCHMARKS=ON`。

## 平台说明

- `build.sh`、`test.sh`、`coverage.sh` 面向 macOS/Linux
- `build.sh`、`test.sh`、`coverage.sh` 现在都按 OS 类型选择并发数探测逻辑，而不是仅靠 `sysctl`/`nproc` 是否存在来判断平台
- `build.sh`、`test.sh`、`coverage.sh` 中的项目内 `.conda-hfilesdk` 也只是可选本地前缀，不是 Linux/macOS 的必需路径；脚本会优先尊重外部传入的 `CMAKE_PREFIX_PATH` / `Arrow_DIR`
- `build.sh`、`test.sh`、`coverage.sh` 现在都会在配置前做前置依赖检查；在 Windows + MSYS2 场景下，会更早提示 `MSYSTEM=CLANG64`、`cmake`、`ctest`、`clang/clang++`、`llvm-cov`、`llvm-profdata` 等条件
- `bench-runner.sh` 采用双平台模式：Linux 默认启用完整 benchmark/HBase 路径；macOS 默认关闭 HBase stage、跳过 CPU 绑核与 page cache drop
- `bench-runner.sh` 需要本地可发现 `google-benchmark`；可通过 `BENCHMARK_PIN` 指向安装前缀，或通过 `CMAKE_PREFIX_PATH` / `benchmark_DIR` 提供 CMake 包路径
- `bench-runner.sh` 中的项目内 `.conda-hfilesdk` 只是可选本地前缀，不是 Linux/macOS 的必需路径；脚本会优先尊重外部传入的 `CMAKE_PREFIX_PATH` / `Arrow_DIR` / `benchmark_DIR`
- Windows 下推荐直接使用 `.bat` 入口；这些脚本会查找 `MSYS2_BASH`、`MSYS2_ROOT`、`C:\msys64\usr\bin\bash.exe`、`C:\msys32\usr\bin\bash.exe`，最后再回退到 `PATH` 中的 `bash.exe`
- 若你在 Windows 上使用 MSYS2 Clang，建议先在对应的 MSYS2 环境中准备好 `cmake`、`clang`、Arrow 依赖以及可选的 `google-benchmark`
- `_run_msys2_script.bat` 默认会把 `MSYSTEM` 设为 `CLANG64`，并在已知 `MSYS2_ROOT` 时把 `%MSYS2_ROOT%\%MSYSTEM%\bin` 与 `%MSYS2_ROOT%\usr\bin` 注入 `PATH`
- `coverage.bat` 会转调 `coverage.sh`；脚本现在会在配置前检查 `cmake`、`clang/clang++`、`llvm-cov`、`llvm-profdata`，并在 Windows + MSYS2 场景下打印更明确的前置提示
