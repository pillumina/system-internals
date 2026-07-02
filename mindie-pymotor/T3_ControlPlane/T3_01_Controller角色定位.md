# T3_01_Controller角色定位

Controller 是 MindIE PyMotor 的**控制面大脑**——它负责集群的全局业务状态管控、PD 实例身份分配、故障检测与隔离、节点配置下发。Coordinator 处理每一个用户请求，是数据面；Controller 处理每一条"实例注册 / 心跳 / 故障 / 配置变更"事件，是控制面。本文梳理 Controller 在 `motor/controller/main.py` 中的入口装配、与 Coordinator 的边界划分。

## 进程模型与启动

`main()`（`motor/controller/main.py:262-351`）是 Controller 进程入口，关键步骤：

1. 加载 `ControllerConfig`（CLI `--config` 或环境变量 `USER_CONFIG_PATH`，`main.py:266-273`）；
2. `apply_controller_ports` 与 `run_port_setup_or_exit` 处理端口冲突；
3. `start_config_file_watcher` 监听配置文件变化，回调 `on_config_updated`（`main.py:283`）；
4. 根据 `standby_config.enable_master_standby` 选择两条路径之一：
   - **启用主备**：`init_all_modules()` 启动 `ControllerAPI`，再启动 `StandbyManager` 协调主备状态（`main.py:290-308`）；`on_become_master` 启动其余模块，`on_become_standby` 停止除 ControllerAPI 外的模块；
   - **禁用主备**：直接 `init_all_modules()` + `start_all_modules()`（`main.py:309-315`）。

`init_all_modules()`（`main.py:140-170`）实例化五大模块：

- `InstanceAssembler`：节点装配器，向 NodeManager 派发实例身份；
- `EventPusher`：实例状态→Coordinator 推送器；
- `FaultManager`（仅当 `fault_tolerance_config.enable_fault_tolerance=True`）；
- `InstanceManager`：实例状态机与心跳管理（详见 `T3_02_InstanceManager.md`）；
- `Observability`（仅当 `observability_config.observability_enable=True`）；
- `ControllerAPI`：对外暴露心跳接收、配置刷新等 HTTP 端点。

模块启动后再用 observers_list 把 `EventPusher`、`FaultManager` 挂到 `InstanceManager` 的观察者列表上（`main.py:134-170`）——这一注入是 Observer 模式生效的前提。

## 与 Coordinator 的边界

Controller 与 Coordinator 的边界划分非常清晰：

| 维度 | Coordinator | Controller |
|------|-------------|------------|
| 角色 | 数据面 | 控制面 |
| 决策 | "这个请求路由到哪台实例" | "实例是 P 还是 D / 该不该隔离" |
| 关注 | 实时 workload、Prefix 命中 | 节点健康、PD 角色、集群拓扑 |
| 输入 | 用户 HTTP 请求、Scheduler 内部信号 | NodeManager 心跳、Controller API 调用 |
| 输出 | OpenAI 兼容响应、HTTP 200/4xx/5xx | 实例事件推送、故障隔离指令 |
| 时延要求 | 强（每个请求） | 弱（秒级） |

二者通过两条通道交互：

1. **Controller → Coordinator 实例事件**：`EventPusher` 把 `InstanceManager` 的状态变更（`INSTANCE_READY`/`INSTANCE_SEPERATED`/`INSTANCE_PAUSED`/`INSTANCE_RESUMED`）打包成 `InsEventMsg`，通过 `CoordinatorApiClient.send_instance_refresh` 投递到 Coordinator 的 `POST /instances/refresh`（详见 `T3_03_EventPusher.md`）；
2. **Controller API ↔ NodeManager / 外部**：NodeManager 心跳、PD 身份查询、Controller 重启事件都走 Controller 的 HTTP API（详见 `T3_05_ConfigResolver.md`）。

控制面绝不参与用户请求路径——它只关心"哪些实例健康、PD 角色是什么、当前期望的集群拓扑长什么样"，并把这些信息推给数据面做调度决策。

## 模块协作概览

```text
NodeManager 心跳 → ControllerAPI → InstanceManager.handle_heartbeat
                                       │
                                       │ 状态机转移
                                       ▼
                                self.notify(ins, event)
                                       │
                  ┌────────────────────┼────────────────────┐
                  ▼                    ▼                    ▼
              EventPusher        FaultManager         Observability
                  │                    │
                  │ InsEventMsg        │ 故障决策
                  ▼                    ▼
        Coordinator POST /instances/refresh   隔离/重启指令
```

InstanceManager 是所有实例状态的"唯一真相源"，EventPusher 与 FaultManager 都是订阅它的 Observer（详见 `T3_04_Observer模式.md`）。FaultManager 仅在 `enable_fault_tolerance=True` 时存在，处理进程崩溃、节点失联等需要"主动隔离 + 触发恢复"的场景。

## 配置文件热更新

`config_watcher = start_config_file_watcher(config, on_config_updated)`（`main.py:283`）使用 `ConfigWatcher`（`motor/common/utils/config_watcher.py`）监控配置文件变化。`on_config_updated()`（`main.py:54-131`）会：

1. 比较 `fault_tolerance_config.enable_fault_tolerance` 前后值，按需启动/停止 FaultManager；
2. 遍历 `modules` 调用 `update_config(config)` 让每个模块更新内部状态；
3. 打印新的配置摘要。

`update_config` 在 EventPusher、InstanceManager 上都做了对应的实现（`event_pusher.py:105-108`、`instance_manager.py:170-180`），保证热更新不会让模块停留在旧配置上。

## 主备切换

`StandbyManager`（`main.py:302-307`）是 Controller 的高可用入口。它通过共享存储（如 etcd）选举 master：

- 当前进程成为 master：`on_become_master` 回调被触发，启动所有非 ControllerAPI 模块；
- 当前进程变为 standby：`on_become_standby` 回调被触发，停掉除 ControllerAPI 外所有模块，让另一个 master 接管。

`on_become_master` 还会通过 `Observability().add_alarm` 上报 `MASTER_COMPONENT_EXCEPTION` 事件，方便运维感知主备切换。

## 跨文档引用

- InstanceManager 的状态机与转换表见 `T3_02_InstanceManager.md`。
- 实例生命周期 INITIAL→ACTIVE→PAUSED→DELETED 见 `T3_02_1_实例生命周期.md`。
- PD 角色动态调整机制见 `T3_02_2_实例身份分配.md`。
- EventPusher 的事件类型与推送时机见 `T3_03_EventPusher.md`。
- Observer 模式在 Controller 中的应用见 `T3_04_Observer模式.md`。
- 多层配置解析的优先级见 `T3_05_ConfigResolver.md`。
