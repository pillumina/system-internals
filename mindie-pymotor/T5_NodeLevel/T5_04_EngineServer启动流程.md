# T5_04 EngineServer 启动流程

## 总览

EngineServer 是部署在 Pod 内的推理引擎进程（典型为 vLLM）。NodeManager 不直接执行推理，而是作为**进程管理者**通过 `Daemon.pull_engine` 拉起 engine_server 子进程。本节梳理从 Controller 分配角色到 engine_server 进程接收请求的完整链路。

涉及代码：

- `motor/node_manager/core/engine_manager.py`：StartCmdMsg 解析与 ranktable 写盘。
- `motor/node_manager/core/daemon.py`：构造 `engine_server` 子进程。
- `motor/node_manager/core/heartbeat_manager.py`：状态轮询、心跳上报。
- `motor/common/resources/http_msg_spec.py`：`StartCmdMsg` 数据契约。

## 端到端时序

```text
Controller.assemble_instance() 完成
        │
        ▼
Controller._send_start_command()   # 通过 HTTP 下发 StartCmdMsg
        │
        ▼
NodeManagerAPI /start（HTTP 接收）
        │
        ▼
EngineManager.parse_start_cmd(start_cmd)
  - _check_cmd_para（job_name / endpoints / pod_ip 一致性）
  - 保存 instance_id、endpoints、d2d_peer_ips、node_rank
  - _write_ranktable_to_file(Env.ranktable_path)
  - 标记 is_snapshot_master（如适用）
        │
        ▼
Daemon.pull_engine(pd_role, endpoints, instance_id, master_dp_ip, ...)
  - 构造 env：POD_IP → VLLM_HOST_IP（若未设置）
  - 若 enable_multi_endpoints，按 local_world_size 计算 ASCEND_RT_VISIBLE_DEVICES
  - 拼接 cmd 列表
  - subprocess.Popen("engine_server", ...)
  - 记录 process.pid
        │
        ▼
engine_server 启动 → 加载模型 → 暴露 inference / mgmt port
        │
        ▼
HeartbeatManager._wait_for_engine_servers_ready(timeout=60)
  - 通过 socket.create_connection 探测 mgmt port
        │
        ▼
HeartbeatManager._refresh_endpoints_status_loop
  - 调用 EngineServerApiClient.query_status → 更新 endpoint.status
        │
        ▼
HeartbeatManager._report_heartbeat_loop
  - 发送 HeartbeatMsg(ins_id, status={...})
        │
        ▼
Controller 收到心跳 → 状态机 INITIAL → ACTIVE
实例对外提供推理服务
```

## StartCmdMsg 构造

Controller 侧 `_send_start_command` 在 `motor/controller/core/instance_assembler.py` 中组装 `StartCmdMsg`，字段含义如下（`motor/common/resources/http_msg_spec.py:StartCmdMsg`）：

| 字段 | 类型 | 来源 |
|------|------|------|
| `instance_id` | `int` | InstanceManager 分配 |
| `job_name` | `str` | 与 NodeManager config 中一致 |
| `role` | `str` | `"prefill"` / `"decode"` / `"union"` |
| `endpoints` | `list[Endpoint]` | 当前 NodeManager 注册的端点 |
| `master_dp_ip` | `str` | 数据并行的 master 节点 IP |
| `ranktable` | `Ranktable` | HCCL 通信所需的 ranktable |
| `node_rank` | `int` | 节点在实例内的序号 |
| `d2d_peer_ips` | `list[str]` | Decode-to-Decode peer 列表 |

校验逻辑 `_check_cmd_para`（`engine_manager.py:284-298`）确认 `start_cmd.job_name == config.basic_config.job_name`、`len(start_cmd.endpoints) == endpoint_num`、每个 endpoint 的 `ip == pod_ip`。校验失败则拒绝拉起引擎并记录日志。

## engine_server 子进程构造

`Daemon.pull_engine`（`daemon.py:87-171`）的关键步骤：

1. **环境变量**：拷贝 `os.environ`，把 `POD_IP` 写入 `VLLM_HOST_IP`（vLLM 习惯用 host_ip）；Moongcake IPv6 实验开关时同步 `MC_USE_IPV6`。
2. **多 endpoint 模式**：按 `_calc_visible_device_ids(i, device_size)`（`daemon.py:188-202`）计算 `ASCEND_RT_VISIBLE_DEVICES`，按 index 切分 NPU。
3. **构造命令行**：
   ```text
   engine_server
       --dp-rank {endpoint.id}
       --instance-id {instance_id}
       --role {prefill|decode|union}
       --host {endpoint.ip}
       --port {endpoint.business_port}
       --mgmt-port {endpoint.mgmt_port}
       --master-dp-ip {master_dp_ip}
       --node-rank {node_rank}
       --config-path {Env.user_config_path}
   ```
   快照模式追加 `--snapshot-metadata`；单容器模式追加 `--kv-port`、`--dp-rpc-port`、`--lookup-rpc-port`；D2D 模式下追加 `--d2d-peer-ips`。
4. **拉起**：用 `subprocess.Popen(cmd, shell=False, env=env)` 启动子进程；`process.poll()` 立即检查返回码，进程立刻退出则抛 `RuntimeError`。
5. **PID 记录**：把 `process.pid` 加入 `Daemon.engine_pids`，便于 `Daemon.stop` 时统一 SIGKILL。

## 启动失败回滚

`Daemon.pull_engine` 在以下情况抛 `RuntimeError`，由调用方（NodeManagerAPI `/start` 路由）捕获并返回 HTTP 5xx：

- endpoint 参数不合法（端口超出 `[1024, 65535]`、IP 解析失败，`daemon.py:60-79`）。
- `subprocess.Popen` 抛出异常。
- 进程 `poll()` 返回非 None（启动后立即退出）。

NodeManager 主进程在子进程失败时不会自动恢复，而是依赖 K8s 重启 Pod；这也是 `main.py:148-153` 中自杀机制存在的意义——保证 engine 失败时整个 Pod 退出而非继续服务陈旧状态。

## 状态回环：ready → 心跳

`HeartbeatManager._wait_for_engine_servers_ready`（`heartbeat_manager.py:198-218`）用 `socket.create_connection` 轮询 mgmt port 直到可连。一旦可达，后续 `_get_engine_server_status` 通过 HTTP `EngineServerApiClient.query_status` 拿到 vLLM 报告的状态，并写回 `endpoint.status`。

第一次状态变化时记录日志（`heartbeat_manager.py:295-301`），并由下一次心跳上报到 Controller。Controller 在收到 `ACTIVE` 状态的 endpoint 后将实例从 `INITIAL` 推进到 `ACTIVE`，进入服务态。

## 与快照模式的关系

快照启用时（`enable_snapshot`），`StartCmdMsg` 不会立即触发 engine 进程；`EngineManager.engine_suspend_prepare`（`engine_manager.py:99-121`）先写入 `model_save_path`，再调用 `Daemon.pull_engine` 拉起子进程，子进程内 `SnapshotSentinel` 接管 suspend 流程（详见 `T5_06_Snapshot机制.md`）。

## 文档交叉引用

- NodeManager 整体结构：`T5_01_NodeManager架构.md`。
- 心跳与状态轮询：`T5_03_HeartbeatManager.md`。
- Controller 侧 StartCmdMsg 下发逻辑：T3_ControlPlane 中的 InstanceAssembler 章节。
- 快照与 engine 暂停/恢复：`T5_06_Snapshot机制.md`。