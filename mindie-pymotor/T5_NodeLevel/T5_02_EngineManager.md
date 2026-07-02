# T5_02 EngineManager

## 职责

`EngineManager`（`motor/node_manager/core/engine_manager.py`）是 NodeManager 内负责**注册协商、启动指令解析、快照协同**的子模块：

1. 启动时向 Controller 注册（`RegisterMsg`）；
2. 接收 Controller 下发的 `StartCmdMsg` 并校验参数；
3. 在快照模式下维护 `model_save_path`、`model_load_path`、`data_parallel_master_ip` 等元数据；
4. 启动或重启时构造 `ReregisterMsg`，把已有的 `instance_id` 与 `endpoints` 上报 Controller 接管。

它是 NodeManager 内**最早上线**的后台模块（`__init__` 立即拉起 `_register_thread`）。

## 注册主循环

`_register()`（`engine_manager.py:300-334`）的执行步骤：

1. `wait_until_api_ready(timeout=30)`：等待 `NodeManagerAPI` 的 HTTP server 启动，避免注册请求打到无效地址。
2. 调用 `post_register_msg()`，最多重试 5 次，间隔指数退避（`retry_interval *= 2`，`engine_manager.py:323`）。
3. 5 次失败后向自身发 SIGTERM 触发 `signal_handler`，由 main 退出并被 K8s 重调度。

`post_register_msg_after_restore()`（`engine_manager.py:216-222`）专用于主机侧快照恢复后的注册，使用新的 `job_name` 与 `pod_ip`；`post_reregister_msg()`（`engine_manager.py:224-230`）用于 Controller 重启后的重注册，携带已有 `instance_id` 与 `endpoints`，由 Controller 决定是否下发 StartCmdMsg（若是直接接管则不会下发）。

## StartCmdMsg 解析

`parse_start_cmd(start_cmd)`（`engine_manager.py:232-249`）：

- 通过 `_check_cmd_para`（`engine_manager.py:284-298`）校验 `job_name` 与 `endpoints` 数量、IP 是否与本地一致，避免错位下发。
- 保存 `instance_id`、`endpoints`、`d2d_peer_ips`、`node_rank` 到本地。
- 若启用快照且未从主机侧快照恢复且 `node_rank == 0`，标记为 `is_snapshot_master`，使该 Pod 负责写入快照元数据。
- 将 `ranktable` 写入 `Env.ranktable_path`，供 HCCL / 引擎进程读取。

`StartCmdMsg` 的字段定义在 `motor/common/resources/http_msg_spec.py`：`instance_id`、`endpoints`（`list[Endpoint]`）、`master_dp_ip`、`ranktable`、`node_rank` 等。

## 快照元数据管理

快照启用时（`enable_snapshot == true`），`EngineManager` 维护三段元数据：

- `model_save_path`：`_call_suspend` 前写盘路径（`engine_suspend_prepare`，`engine_manager.py:99-121`）。
- `model_load_path`：恢复后加载路径（`engine_resume_prepare`，`engine_manager.py:180-206`）。
- `data_parallel_master_ip`：恢复时使用的 master DP IP（`engine_resume_prepare` 同上）。

`register_prepare_after_restore()`（`engine_manager.py:139-178`）在主机侧快照恢复后用 `snapshot_metadata.json` 中的 `job_name`、新的 `pod_ip` 刷新本地 config，并把 `controller_api_dns` 重建为 `<host>.<restored_namespace>.svc.cluster.local`。

`get_snapshot_metadata_path()`（`engine_manager.py:86-97`）优先返回显式配置的 `snapshot_metadata_path`；若 ConfigMap 中挂载了 `snapshot_metadata.json`，先拷贝到默认路径再返回，避免写入挂载的只读卷。

## 与 ControllerApiClient 的边界

`ControllerApiClient.register(register_msg)`、`re_register(reregister_msg)`、`register_after_restore(register_msg)` 都是同步 HTTP 调用，返回 `bool | None`。EngineManager 不重试 HTTP 层错误，仅重试整个注册流程（重试间隔逐次翻倍）。

## 与 FaultReporter 的协作

`EngineManager.__init__`（`engine_manager.py:61-67`）构造 `FaultReporter(config)`，`start()` 时调用 `fault_reporter.start(self.endpoints)` 把当前 endpoints 注入。`update_config`（`engine_manager.py:74-84`）转发到 `FaultReporter.update_config(config, self.endpoints)`，由后者决定是否需要重建 ZMQ 订阅。

## 文档交叉引用

- 启动流程与 main 协同：`T5_01_NodeManager架构.md`。
- StartCmdMsg 端到端流转：`T5_04_EngineServer启动流程.md`。
- 快照元数据生命周期：`T5_06_Snapshot机制.md`。