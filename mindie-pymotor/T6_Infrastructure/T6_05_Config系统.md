# T6_05 Config 系统：Controller / Coordinator / NodeManager 的配置层级

Motor 的运行时配置由三层结构组成：每个组件独立的根 dataclass（`ControllerConfig / CoordinatorConfig / NodeManagerConfig`）、若干子配置 dataclass（Logging / API / TLS / Etcd / 故障容错…）、以及统一的 JSON 序列化与样例生成工具 `generate_config_sample.py`。本节梳理这套配置体系如何做到「字段可分组 / 类型可校验 / 可重载 / 可输出」。

## 一、ControllerConfig：管理面配置

`motor/config/controller.py:130-399` 的 `ControllerConfig` 是 Controller 进程的根配置。它把配置按职责拆成 12 个子配置对象（`controller.py:134-148`）：

| 子配置 | 关键字段 | 用途 |
|--------|---------|------|
| `logging_config` | log_level, log_format, host_log_dir | 日志（见 T6_03） |
| `api_config` | controller_api_host/port, observability_api_port | HTTP 服务监听 |
| `mgmt_tls_config / etcd_tls_config / grpc_tls_config / observability_tls_config` | enable_tls, ca/cert/key 文件路径 | 各通道 TLS |
| `instance_config` | assemble_timeout, heartbeat_timeout, expired_timeout | 实例生命周期 |
| `event_config` | coordinator_heartbeat_interval | 事件推送 |
| `fault_tolerance_config` | enable_fault_tolerance, scale_p2d, token_reinference, scale_p2d_d_instance_reinit_wait_timeout | 高级 RAS 策略 |
| `observability_config` | observability_enable, metrics_ttl | 可观测性开关 |
| `standby_config` | enable_master_standby, master_lock_key/ttl | 主备选举 |
| `etcd_config` | etcd_host/port/timeout, enable_etcd_persistence | etcd 连接 |
| `port_allocator_config` | enable, bind_host, scan_range | 端口分配策略 |
| `precision_auto_recovery_enabled` | bool | 精度异常自动恢复 |

### from_json：合并默认与用户配置

`from_json` (`controller.py:158-250`) 是核心加载流程：

1. 用 `resolve_config_json_path` 找到配置文件路径；
2. 读 JSON；若顶层有 `motor_controller_config` 键则只取这一段，否则视为整个 JSON 都是该组件配置；
3. 调用 `_update_tls_config` 把 TLS 字段在四个子配置间正确归位——TLS 是顶层字段，但配置根上分了四个槽；
4. `cls()` 构造默认实例；
5. 对每个子配置键，如果 JSON 里有对应段，调 `update_config_from_dict` 仅修改 dataclass 已存在的字段（避免外部 JSON 注入未声明字段）；
6. `apply_config_path_metadata` 设置 `config_path / last_modified`；
7. `apply_standby_persistence_rule` 实现「启用了 master/standby 就自动开 etcd 持久化」的策略；
8. `reconfigure_logging` 用新 LoggingConfig 重配全局 logger；
9. `finalize_json_config_load` 打印加载结果摘要。

注意 `controller.py:235` 有个历史遗留——两次 `config.last_modified = None` 紧挨着写，应该合并成一行；但功能上不造成问题。

### validate_config：守住配置边界

`validate_config` (`controller.py:252-315`) 集中校验数值范围：

- 日志级别必须在 `['DEBUG','INFO','WARNING','ERROR']`；
- 端口必须 1-65535；
- 各 timeout 必须 > 0；
- `scale_p2d_d_instance_reinit_wait_timeout` 必须 1-600（10 分钟上限）；
- 所有错误用 `errors: list[str]` 累积，最后 `raise_if_config_errors` 一次性抛——避免半成功半失败的状态。

### reload / to_dict / save_to_json / get_config_summary

四个工具方法让 ControllerConfig 可以：

- `reload()` (`controller.py:317-323`)：在不重启的情况下重新读 JSON 配置。底层走 `reload_dataclass_config_from_json`，可跳过私有字段；
- `to_dict()` (`controller.py:325-335`)：用 `dataclasses.asdict` 转字典，移除 `config_path / last_modified` 内部字段；
- `save_to_json()` (`controller.py:337-345`)：把当前内存配置反向写回文件，用于「UI 改配置后回写」；
- `get_config_summary()` (`controller.py:347-399`)：启动时打印的 ASCII 树形摘要——分层展示 logging / network / instance / HA / observability。

## 二、CoordinatorConfig：数据面配置

`motor/config/coordinator.py:349-860` 的 `CoordinatorConfig` 与 ControllerConfig 结构相似，但子配置更密集（19 个）。显著差异：

### 更多与调度相关的子配置

- `scheduler_config` (`coordinator.py:125-146`)：包含 `SchedulerType` 枚举（`LOAD_BALANCE / ROUND_ROBIN / KV_CACHE_AFFINITY`）和 kv_affinity 调参；
- `inference_workers_config.num_workers`：API 进程数（>1 即多进程）；
- `prometheus_metrics_config` (`coordinator.py:149-155`)：Prometheus 抓取相关；
- `exception_config` (`coordinator.py:158-184`)：重试 / 异常 / 超时配置；
- `api_key_config` (`coordinator.py:218-239`)：API Key 鉴权，含 PBKDF2_SHA256 加密设置；
- `rate_limit_config` (`coordinator.py:273-287`)：限流，可选 simple / olc 后端；
- `prefill_kv_event_config` (`coordinator.py:331-345`)：Mooncake Conductor 集成。

### AIGW 自动装配

`coordinator.py:467-481` 处理一种特殊 JSON 结构：当用户传入完整 user_config_data（含 `motor_engine_prefill` / `motor_engine_decode`），`CoordinatorConfig.from_json` 从中抽取 `model_name / max_model_len` 等自动填充 `aigw_model` 段——这是「OpenAI 兼容网关」自动暴露 `/v1/models` 的来源。

### 弃用字段处理

`coordinator.py:443-465` 显式给出兼容策略：

- `recompute_enabled`：被 `reschedule_enabled` 替代，保留 setter 仅打 warning；
- `recompute_max_retry`：完全移除，只打 warning。

这是「优雅迁移」的样板——保留旧键的可解析性，但不引入新的语义。

### Standby 通过共享内存（Shm）同步

`coordinator.py:66-71` 定义 `ROLE_SHM_NAME = "coordinator_standby_role"` 与 9 字节布局：

```
byte0: role (1=master, 0=standby)
bytes 1-8: heartbeat (uint64 little-endian)
```

Coordinator 的 standby 模块（Daemon/Mgmt）通过读写同一段共享内存同步角色，比走 etcd 快几个数量级——这是本地进程间通信的优化点。

## 三、NodeManagerConfig：节点级配置

`motor/config/node_manager.py:254-714` 的 `NodeManagerConfig` 是 EngineServer 侧的配置入口。它有两个独特点：

### 角色相关加载

`from_json` (`node_manager.py:289-328`) 通过 `Env.role` 决定读 `motor_engine_encode_config / motor_engine_prefill_config / motor_engine_decode_config / motor_engine_union_config` 中哪个 key 下的 `motor_nodemanger_config` 段——一个 JSON 文件能容纳多种角色配置，NodeManager 只取自己对应的那一段。

### Dispatch capability 推断

`_infer_dispatch_capabilities` (`node_manager.py:432-454`) 从引擎原生配置（vLLM kv_transfer_config 或显式 dispatch_profile）自动推出 Motor 自己的 dispatch capability 列表。同时 `_discard_user_dispatch_capabilities` (`node_manager.py:330-361`) 主动丢弃用户写在 JSON 里的 `dispatch_capabilities` 字段并打 warning——因为这个字段已经由底层引擎语义决定，不应让用户覆盖。

### 单容器模式

`SingleContainerNodemanagerConfig.from_json` (`node_manager.py:167-243`) 处理一种特殊部署：prefill / decode / encode 三种角色跑在同一个容器里，需要复杂的 port 偏移计算（`d_node_manager_port_offset`, `d_base_port_offset`, `d_device_offset` 等）——这段是「同一个 host 上启动三个角色实例」的偏移算法核心。

### 跨节点 PCP

`node_manager.py:405-420` 处理 `nnodes > 1 && pcp_size % nnodes == 0` 的跨节点 PCP 场景：把每个节点的 `local_world_size` 从全量 `pcp_size * tp * pp` 调整为 `pcp_size / nnodes * tp * pp`——这是「每个节点只持有部分 PCP rank」的正确做法。

## 四、generate_config_sample.py：统一样例生成

`motor/config/generate_config_sample.py:21-72` 的 `write_sample_json` 把三个组件的配置合并成一份样例 JSON：

1. 调 `ControllerConfig / CoordinatorConfig / NodeManagerConfig().to_dict()` 拿到三个默认字典；
2. 把 5 个 TLS 子配置统一抽到顶层 `motor_deploy_config.tls_config`——样例文件结构扁平化，便于用户编辑；
3. 把 `deploy_config` 从 coordinator 中拆出来，与 `tls_config` 合并成完整的部署块（`p_instances_num, d_instances_num, image_name, weight_mount_path, job_id, hardware_type, …`）；
4. 输出顶层结构：

```json
{
  "version": "v2.0",
  "motor_deploy_config": {...},
  "motor_controller_config": {...},
  "motor_coordinator_config": {...},
  "motor_nodemanger_config": {...}
}
```

注意顶层键名 `motor_nodemanger_config` 是历史拼写错误（应该是 `node_manager`），但因为生产配置已经在用这个键名，没改——这是个典型的「API 兼容性优先于命名正确性」的例子。

## 五、模块关系

- **与 Logger（T6_03）**：每个组件配置都内嵌 `LoggingConfig`，`from_json` 流程末尾调用 `reconfigure_logging` 把 JSON 里的 log_level / log_format 立即生效；
- **与 PortAllocator（T6_04）**：`port_allocator_config` 是 Config 的字段，启动序列在 `from_json` 之后调 `apply_*_ports(config)` 原地修改 `api_config` 的端口字段；
- **与 Alarm（T6_06）**：`fault_tolerance_config.scale_p2d_d_instance_reinit_wait_timeout` 等字段由 AlarmStore 的告警路径消费；
- **与 HTTP（T6_02）**：四个 TLS 子配置（mgmt / infer / etcd / grpc / observability）分别对应 HTTP 客户端、etcd gRPC 通道的 TLS 开关。