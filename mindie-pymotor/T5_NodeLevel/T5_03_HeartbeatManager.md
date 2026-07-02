# T5_03 HeartbeatManager

## 职责

`HeartbeatManager`（`motor/node_manager/core/heartbeat_manager.py`）负责两件事：

1. **心跳上报**：周期性向 Controller 报告本节点所有 endpoint 的状态。
2. **endpoint 状态轮询**：通过 engine_server 的 mgmt port 拉取每个 endpoint 的实时状态，决定下一次心跳中的 `status` 字段。

它还隐含承担**自杀检测**：连续 5 次心跳异常（endpoint 处于 `ABNORMAL`）时设置 `_should_suicide`，由 `main.py` 主动退出进程（参见 `T5_01_NodeManager架构.md`）。

## 心跳消息格式

心跳通过 `ControllerApiClient.report_heartbeat` 上报，消息体定义在 `motor/common/resources/http_msg_spec.py:HeartbeatMsg`：

| 字段 | 类型 | 说明 |
|------|------|------|
| `job_name` | `str` | 与 EngineManager 中一致 |
| `ins_id` | `int` | instance_id（首次注册前为 `-1`） |
| `ip` | `str` | 节点 Pod IP |
| `status` | `dict[int, EndpointStatus]` | 按 endpoint id 给出状态 |

`EndpointStatus`（`motor/common/resources/endpoint.py`）当前取值 `NORMAL` / `ABNORMAL` / `PAUSED`。`PAUSED` 由 NodeManager 在 PreStop 时手动设置（`pause_all_endpoints`，`heartbeat_manager.py:153-164`），用于触发 Controller 的实例 RESUME 流程。

## 上报循环

`_report_heartbeat_loop`（`heartbeat_manager.py:310-382`）的执行步骤：

1. 在 `self._endpoint_lock` 内快照当前 endpoints 与状态。
2. **快照模式特殊处理**：
   - 若 `not is_restored_from_host_side_snapshot()` 且 `not EngineManager().is_engine_checkpoint_done()`，跳过本轮上报，避免在 checkpoint 完成前上报导致 Controller 误判（`heartbeat_manager.py:324-334`）。
   - 若 `is_restored_from_host_side_snapshot()` 且尚未注册新 job_name，先调用 `_register_after_restore()` 再 sleep（`heartbeat_manager.py:337-341`）。
3. 构造 `HeartbeatMsg` 并调用 `ControllerApiClient.report_heartbeat`。
4. **异常路径**：
   - 收到 `503`：视为 Controller 重启，调用 `_reregister()`。
   - 快照恢复未完成注册：再次触发 `_register_after_restore()`。
5. 维护 `_consecutive_abnormal_count`：异常累加、正常清零；达到 5 次时设置 `_should_suicide`。

`update_endpoint(StartCmdMsg)`（`heartbeat_manager.py:97-111`）在 EngineManager 解析 StartCmdMsg 后被调用，把 `instance_id`、`endpoints`、`_job_name`、`_role` 同步到 HeartbeatManager，并清零 `_consecutive_abnormal_count` 与 `_should_suicide`。

## endpoint 状态轮询

`_refresh_endpoints_status_loop`（`heartbeat_manager.py:191-196`）单线程，每秒轮询一次。先调用 `_wait_for_engine_servers_ready(timeout=60)`（`heartbeat_manager.py:198-218`）确保 mgmt port 可连，否则等待 socket 可达或 engine 进程消失。

`_get_engine_server_status()`（`heartbeat_manager.py:220-308`）逐个 endpoint 调用 `EngineServerApiClient.query_status`，把返回的 `status` 转换为 `EndpointStatus`：

- `_is_within_grace_period`（启动后 120s 内，`heartbeat_manager.py:229-231`）内检测到异常时**保留原状态**，避免冷启动抖动导致自杀。
- `EndpointStatus.PAUSED` 一旦设置就不再被覆盖（PreStop 期间的状态保护）。
- 主机侧快照恢复后且 `pod_ip` 未刷新前，保留 stale status，等恢复流程完成。

## 自杀机制

`should_suicide()`（`heartbeat_manager.py:113-119`）仅返回标志位，不做副作用。`main.py` 在主循环里读取后调用 `suicide_procedure()`（详见 `T5_01_NodeManager架构.md`）。该机制避免 NodeManager 在 engine 全面故障时仍持续尝试上报造成日志风暴和资源浪费。

## Controller 重启检测

`_reregister()`（`heartbeat_manager.py:400-405`）调用 `EngineManager.post_reregister_msg()`，由后者构造 `ReregisterMsg`（含已有 `instance_id` 和 `endpoints`）提交 Controller。Controller 据此决定：

- 若 Controller 重启后能从 ETCD 重建实例视图，则无需下发 StartCmdMsg。
- 否则下发新 StartCmdMsg 触发引擎重启。

## 文档交叉引用

- 注册/重注册消息格式与构造：`T5_02_EngineManager.md`。
- NodeManager 主循环与自杀触发：`T5_01_NodeManager架构.md`。
- 状态机的 ACTIVE/INACTIVE 转换：T3_ControlPlane 中的 InstanceManager 章节。