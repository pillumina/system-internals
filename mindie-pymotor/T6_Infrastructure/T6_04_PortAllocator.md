# T6_04 PortAllocator：端口分配策略与冲突规避

Motor 各组件（Controller、Coordinator、NodeManager、EngineServer）启动时需要在同一节点上占用多个 TCP 端口，而这些端口必须互不冲突、并且对运维可观测。`motor/common/utils/port_allocator.py` 实现了三种分配策略以及按组件批量应用的封装函数。

## 一、PortAllocator 三种策略

`port_allocator.py:72-148` 的 `PortAllocator` 是一个静态方法集合，所有方法都基于「在指定 host 上尝试 bind 一个端口」这一原语。

### probe_tcp：基础探测

`probe_tcp(host, port, timeout)` (`port_allocator.py:74-86`) 创建临时 socket 尝试 `bind + listen`：

```python
sock = socket.socket(detect_family(bind_host), socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind((bind_host, port))
sock.listen(1)
return True
```

`detect_family` 来自 `motor/common/utils/net.py`，根据 host 字符串自动选 `AF_INET` 或 `AF_INET6`——这对 IPv6 单栈节点尤其重要。`SO_REUSEADDR` 是为了让处于 `TIME_WAIT` 状态的端口也能被探测使用。

### allocate_strict：硬约束

`allocate_strict` (`port_allocator.py:89-92`) 适用于「必须用这个端口，不能让步」的关键端口，比如 Controller 的对外 mgmt API。如果 `probe_tcp` 返回 False，抛出 `PortConflictError`——这是「该端口被占，进程必须退出」的语义。`run_port_setup_or_exit` (`port_allocator.py:327-332`) 包装了「异常 → 记录日志 → sys.exit(1)」流程，确保硬约束失败时进程优雅退出而不是带着坏状态继续运行。

### allocate_auto：弹性分配

`allocate_auto` (`port_allocator.py:95-117`) 在 `port, port+scan_range)` 区间内寻找第一个可用端口：

```python
for candidate in range(port, port + scan_range):
    if candidate in blocked:
        continue
    if PortAllocator.probe_tcp(host, candidate, timeout=timeout):
        ...
        return candidate
raise PortConflictError(...)
```

`skip_ports` 参数允许把某些已知占用或保留的端口从扫描中排除。这个机制在 NodeManager 场景下至关重要——一个节点要分配 N 个 EngineServer 的 service/mgmt 端口，必须避免给后续端口分配与已分配冲突。

### allocate_auto_broadcast：分配 + 广播

`allocate_auto_broadcast` (`port_allocator.py:120-133`) 在 `allocate_auto` 之上加了一层「分配成功后调用 callback 通知其他进程」。它用于分布式场景下「抢到端口的进程立刻通过 etcd 等中间件广播，让其他节点知道这个端口已经不可用」。如果广播失败，抛出 `PortConflictError`——避免「本地以为占了端口，但其他进程不知道」的双发场景。

### check_remote_reachable：被动连通性检查

`check_remote_reachable(host, port, timeout)` (`port_allocator.py:136-146`) 不是分配端口，而是验证远端 `host:port` 是否可达。Coordinator 用它来检测 Mooncake Conductor 服务是否启动（`port_allocator.py:215-232`），如果不可达只 `logger.warning` 不报错——让服务可以异步启动。

## 二、PortRow 与 print_matrix：可视化

`PortRow` (`port_allocator.py:28-35`) 是个 frozen dataclass，记录一次分配的结果：

```python
component: str   # Coordinator / Controller / NodeManager / EngineServer / Conductor
bind_host: str
port: int
proto: str       # 固定 "TCP"
strategy: str    # strict / auto / remote
purpose: str     # 用途描述
```

`print_matrix(rows)` (`port_allocator.py:38-58`) 把所有分配结果以 ASCII 表格打到日志：

```
[Port Matrix] ================================================================
[Port Matrix] Component     Bind Host       Port    Proto   Strategy    Purpose
[Port Matrix] ----------------------------------------------------------------
[Port Matrix] Controller    10.0.0.1        1026    TCP     strict     mgmt API (external)
[Port Matrix] ================================================================
```

这种「启动一张总表」让运维一眼看到所有关键端口与策略。如果后面发生端口冲突，从这张表对照实际监听情况（`ss -tlnp`）就能快速定位。

## 三、apply_coordinator_ports / apply_controller_ports / apply_node_manager_ports

三个组件的端口应用函数结构一致，但分配策略各异。

### Coordinator 端口分配

`apply_coordinator_ports` (`port_allocator.py:178-242`) 处理四个关键端口：

| 端口 | 策略 | 用途 |
|------|------|------|
| `coordinator_api_infer_port` | strict | 对外推理 API，必须独占 |
| `coordinator_api_mgmt_port` | auto | 管理 API，可弹性 |
| `coordinator_obs_port` | auto | 可观测性 API |
| `prefill_kv_event_config.http_server_port` | auto | Conductor 回调 HTTP（仅当 conductor_service 非空时分配） |

strict 与 auto 的边界在这里很清晰：对外提供服务的端口（infer）必须固定，以便客户端配置连接；内部管理端口（mgmt / obs）可以让步。

### Controller 端口分配

`apply_controller_ports` (`port_allocator.py:245-271`) 相对简单：

- `controller_api_port` strict（对外）
- `observability_api_port` auto

### NodeManager 端口分配：批量约束

`apply_node_manager_ports` (`port_allocator.py:274-324`) 是最复杂的——一个 NodeManager 下挂着 N 个 EngineServer，每个 EngineServer 又有 service port（DP 业务端口）与 mgmt port（DP 管理端口），还要兼容 `single_container_flag` 下的额外端口（`kv_port`, `lookup_rpc_port`, `dp_rpc_port`）。

实现思路：

1. **预计算 reserved 集合**（`port_allocator.py:284-287`）：把 `node_manager_port` + 已有 `service_ports / mgmt_ports` 全部塞进 `reserved`；
2. **逐个 auto 分配**：内部闭包 `_auto(pref, name)` (`port_allocator.py:290-295`) 在分配第 i 个端口时把前面已经分配的端口加入 `allocated`，避免新分配与历史分配冲突；
3. **追加到 rows**：每分配一个端口都同步打印到 Port Matrix；
4. **覆盖回 config**：分配完成后用 `new_service_ports / new_mgmt_ports` 替换原列表，并写回 `endpoint_config`——这是「in-place 修改」模式，调用方拿到的 config 对象被原地更新。

`run_port_setup_or_exit(apply_fn, config)` 是统一入口：`apply_fn` 是 `apply_*_ports`，`config` 是对应的 Config 对象。失败时 `sys.exit(1)`——把端口分配错误提到启动期早失败，避免运行中才发现端口不可用导致反复重启。

## 四、PortAllocatorConfig：可参数化

实际调用前，`PortAllocatorConfig`（在 `motor/config/port_allocator_config.py`）控制：

- `enable`：总开关，false 时 `apply_*` 直接 `return` 不分配；
- `bind_host`：bind 的 host 字面量；
- `scan_range`：auto 策略扫描窗口；
- `probe_timeout_seconds`：单次 bind 探测超时；
- `remote_check_timeout_seconds`：远端连通性检查超时。

## 五、与 Config 系统的对接

三个组件的 Config 类都有 `port_allocator_config` 字段：

- `ControllerConfig.port_allocator_config` (`controller.py:146`)
- `CoordinatorConfig.port_allocator_config` (`coordinator.py:372`)
- `NodeManagerConfig.port_allocator_config` (`node_manager.py:267`)

启动序列大致是：加载 config → 加载 TLS → 调 `apply_*_ports(config)` → 用 config 启动服务。由于 `apply_*_ports` 是原地修改，Port Matrix 反映的是「最终实际生效」的值，与启动后 `ss -tlnp` 看到的一致——可观测性闭环成立。

## 六、模块关系

- **与 Logger**：Port Matrix 是 `logger.info` 输出（`port_allocator.py:42`），靠 logger 系统（T6_03）格式化与轮转。
- **与 HTTP**：分配出来的端口会被 `SafeHTTPSClient` 或 `HTTPClientPool`（T6_02）用于 client 连接；同时 `apply_*_ports` 绑定的 server socket 会被 `uvicorn` / 自定义 server 接管。
- **与 Config**：`ControllerConfig.get_config_summary` (`controller.py:347`) 把 `controller_api_port` 等端口信息拼进启动摘要，与 Port Matrix 互为补充：摘要给人看，Matrix 给运维排障看。