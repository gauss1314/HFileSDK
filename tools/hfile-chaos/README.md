# hfile-chaos

用于验证 HFile 写入链路在故障场景下不会在最终输出路径留下损坏文件。

当前支持两种模式：

- `kill-during-write`：子进程使用 `FsyncPolicy::Safe` 持续写入，父进程在 `.tmp` 文件开始增长后强制 `SIGKILL`
- `disk-full-sim`：通过 `min_free_disk_bytes` 与 `disk_check_interval_bytes` 触发磁盘空间不足错误

构建完成后可直接运行：

```bash
./build/hfile-chaos --mode kill-during-write --output-dir /tmp/hfile-chaos-kill --verify-no-corrupt-files
./build/hfile-chaos --mode disk-full-sim --output-dir /tmp/hfile-chaos-disk --verify-no-corrupt-files
```

`kill-during-write` 只验证 `FsyncPolicy::Safe` 的原子可见性语义，不代表 `Fast` 或 `Paranoid` 模式也具备相同保证。

`--verify-no-corrupt-files` 会检查最终输出目录中不存在可见的损坏 HFile；随后还会验证清理 `.tmp/` 后可在同目录再次成功写入。
