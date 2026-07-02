# T5_06 Snapshot 机制

## 目标

容器快照（Snapshot）是 EngineServer 的一种**进程级暂停/恢复**能力：把推理进程的运行时状态（KV cache、模型权重映射、连接状态）落盘到 host 侧共享卷，使得推理服务可以在新的 Pod 启动后从快照快速恢复，避免重复执行耗时的模型加载与权重解码。该机制对 K8s 的快速弹性扩缩和热迁移至关重要。

实现分布在三处：

- `motor/engine_server/core/snapshot_monitor.py`：标志位单例。
- `motor/engine_server/core/snapshot_sentinel.py`：负责触发 suspend / checkpoint / resume。
- `motor/node_manager/core/engine_manager.py` 与 `motor/node_manager/core/heartbeat_manager.py`：NodeManager 侧的快照协同。

## SnapshotMonitor

`SnapshotMonitor`（`snapshot_monitor.py:16-52`）是单例（继承 `ThreadSafeSingleton`），对外暴露三个布尔标志：

| 属性 | 含义 |
|------|------|
| `is_suspend_done` | EngineServer 已完成 `POST /suspend`，模型权重落盘 |
| `is_unlock_done` | 冷启动到达 checkpoint 后已 `POST /device_unlock` |
| `is_resume_done` | 主机侧快照恢复后已 `POST /resume` 并装载权重 |

写入通过 `mark_*_done()` 在 `_flags_lock` 内完成。`SnapshotSentinel` 在三个阶段分别置位，NodeManager 与 EngineServer 自身根据这些标志决定是否上报心跳或对外提供服务。

## SnapshotSentinel 主流程

`SnapshotSentinel`（`snapshot_sentinel.py:35-200`）是 EngineServer 内的一个守护线程，名字固定为 `snapshot-sentinel`，由 EngineServer 在启动时实例化。

`run()` 串行执行三个阶段：

1. `_wait_until_infer_healthy()`（`snapshot_sentinel.py:70-88`）：通过 `SafeHTTPSClient` 轮询 `GET /health`，确认推理服务可用。
2. `_call_suspend()`（`snapshot_sentinel.py:123-156`）：从 `snapshot_metadata` 中读取 `model_save_path`，调用 `POST /suspend?model_save_path=...`，超时 1 小时（`SUSPEND_TIMEOUT=3600.0`）。成功后 `mark_suspend_done`。
3. `_reach_checkpoint()`（`snapshot_sentinel.py:90-121`）：轮询 `snapshot_metadata` 的 `checkpoint` 字段：
   - 冷启动且 `checkpoint == "done"`：调用 `POST /device_unlock`，`mark_unlock_done`，退出 sentinel（说明已经到达可恢复点）。
   - 主机侧快照恢复：跳出等待进入 resume。
4. `_call_resume()`（`snapshot_sentinel.py:158-200`）：读取 `model_load_path` 与 `data_parallel_master_ip`，调用 `POST /resume?model_path=...&data_parallel_master_ip=...`，超时 1 小时（`RESUME_TIMEOUT=3600.0`）。成功后 `mark_resume_done`。

所有阶段的失败都会通过 `RETRY_LOG_FREQUENCY` 节流日志，避免重试期间刷屏；`_stop_event` 用于 EngineServer shutdown 时强制退出。

## NodeManager 侧协同

### 快照元数据写入

`EngineManager.engine_suspend_prepare`（`engine_manager.py:99-121`）在 `enable_snapshot` 时确保 `MOTOR_SNAPSHOT_WORKSPACE_DIR` 与 `MOTOR_SNAPSHOT_WEIGHT_DIR` 存在，并写入 `model_save_path`。`engine_resume_prepare`（`engine_manager.py:180-206`）在 resume 时写入 `model_load_path` 与 `data_parallel_master_ip`，保证 sentinel 能取到正确的恢复参数。

### 注册与重注册

冷启动（无快照）路径：`_register` → 正常 `post_register_msg`。

主机侧快照恢复路径：EngineManager 通过 `register_prepare_after_restore`（`engine_manager.py:139-178`）用 `snapshot_metadata.json` 里的 `job_name` 刷新本地 config，并把 `pod_ip` 替换为当前 Pod 的 `get_pod_ip()`；然后 HeartbeatManager 检测到 `is_restored_from_host_side_snapshot()`（`heartbeat_manager.py:337-341`）后调用 `_register_after_restore()`，用新的 `job_name` 注册到 Controller。

### 心跳屏障

HeartbeatManager 在 `_report_heartbeat_loop`（`heartbeat_manager.py:310-382`）内做了两个与快照相关的"屏障"：

1. **checkpoint 屏障**：冷启动且 `not EngineManager().is_engine_checkpoint_done()` 时不上报心跳，避免 Controller 把尚未 suspend 的 Pod 当作 `INITIAL/ACTIVE` 实例调度。
2. **快照恢复屏障**：主机侧快照恢复期间必须先完成 `_register_after_restore()`，否则心跳会被抑制。

### 心跳上报期间的卡顿保护

`is_engine_checkpoint_done()`（`engine_manager.py:123-137`）从 `snapshot_metadata` 读取 `checkpoint` 字段；非 `'done'` 时 HeartbeatManager 不上报心跳（`heartbeat_manager.py:325-334`）。这保证了 checkpoint 完成前所有依赖"心跳驱动"的调度行为都不会被错误触发。

## Pod 生命周期与快照的对应关系

| 阶段 | NodeManager 行为 | EngineServer 行为 |
|------|------------------|--------------------|
| 冷启动，无快照 | 正常注册 → StartCmd → 拉起 engine | `_wait_until_infer_healthy` → 进入服务态 |
| 冷启动 + 启用快照 | 同上，但 checkpoint 完成前抑制心跳 | 启动 → suspend → 等待 checkpoint → device_unlock |
| 主机侧快照恢复 | 用 snapshot metadata 刷新 config → 重注册（不经过 StartCmd） | _wait_until_infer_healthy → resume → 装载权重 |
| 异常：checkpoint 永不完成 | 心跳持续被抑制，由 K8s 重启 Pod 兜底 | sentinel 持续轮询 `checkpoint` |

## 文档交叉引用

- NodeManager 启动与模块协同：`T5_01_NodeManager架构.md`。
- 引擎进程拉起：`T5_04_EngineServer启动流程.md`。
- 心跳屏障细节：`T5_03_HeartbeatManager.md`。
- 快照元数据写入路径：`motor/node_manager/core/engine_manager.py:99-206`。