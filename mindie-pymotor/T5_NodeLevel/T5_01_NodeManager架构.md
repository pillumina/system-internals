# T5_01 NodeManager 架构

## 角色定位

NodeManager 是部署在每个推理 Pod 内的**节点级代理进程**，对上承接 Controller 的 `RegisterMsg` / `StartCmdMsg`，对下管理 EngineServer 子进程的生命周期，并提供心跳上报、软件故障转发、容器快照协同等能力。它是控制面与数据面之间的"桥梁进程"。

入口为 `motor/node_manager/main.py`，模块组织如下：

| 模块 | 路径 | 职责 |
|------|------|------|
| NodeManagerAPI | `motor/node_manager/api_server/node_manager_api.py` | HTTP API（`/node-manager/stop`、`/node-manager/pause` 等） |
| Daemon | `motor/node_manager/core/daemon.py` | 拉起 engine_server 子进程，记录 PID |
| EngineManager | `motor/node_manager/core/engine_manager.py` | 注册/重注册、解析 StartCmdMsg、快照元数据 |
| HeartbeatManager | `motor/node_manager/core/heartbeat_manager.py` | 心跳上报与 endpoint 状态轮询 |
| FaultReporter | `motor/node_manager/core/fault_reporter.py` | ZMQ SUB 监听引擎故障并上报 Controller |

## 启动流程

`main()`（`main.py:122-176`）的总体步骤：

1. 注册 SIGINT / SIGTERM 信号处理器（`signal_handler`，`main.py:90-101`），第一次信号触发 `_should_exit=True` 并执行 `stop_all_modules`；第二次信号会被忽略（幂等保护）。
2. 调用 `init_all_modules(config_path)`（`main.py:63-76`）依次构造：
   - `config = NodeManagerConfig.from_json(config_path)`；
   - `run_port_setup_or_exit(apply_node_manager_ports, config)`：校验并分配端口，失败直接退出；
   - `NodeManagerAPI`、`Daemon`、`EngineManager`、`HeartbeatManager`，统一加入 `modules` 列表。
3. 启动 `ConfigWatcher` 监听配置文件变更（`main.py:137-142`）。**快照模式下禁用 inotify**，因为容器快照不支持 inotify 操作。
4. 进入主循环（`main.py:148-163`）：
   - 每轮先检查 `HeartbeatManager().should_suicide()`——若 5 次连续心跳异常则自杀（`main.py:150-153`）；
   - 读取 stdin 命令，支持 `stop` 关键字退出。

`suicide_procedure()`（`main.py:104-119`）用于异常路径：停 ConfigWatcher、停所有模块。返回码约定：`-1` 表示需要 K8s 重新调度 Pod，`0` 表示正常退出。

## 模块协作关系

```text
                   ┌──────────────────────────┐
                   │   Controller (HTTP)      │
                   └────────────┬─────────────┘
                                │ RegisterMsg / StartCmdMsg / HeartbeatMsg
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│ NodeManager (main.py)                                           │
│                                                                 │
│  ┌──────────────────┐    ┌──────────────────┐    ┌────────────┐  │
│  │ EngineManager    │    │ HeartbeatManager │    │ FaultRepor │  │
│  │ - 注册/重注册      │    │ - 心跳上报        │    │ - ZMQ监听  │  │
│  │ - 解析 StartCmd   │    │ - endpoint轮询    │    │ - 软故障上报│  │
│  └────────┬─────────┘    └────────┬─────────┘    └─────┬──────┘  │
│           │                       │                    │         │
│           ▼                       ▼                    ▼         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │             Daemon (subprocess.Popen engine_server)       │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## 配置与运行时热更新

`on_config_updated`（`main.py:49-60`）在 ConfigWatcher 触发时遍历 `modules`，对每个实现 `update_config(config)` 的模块应用新配置。各模块的 `update_config` 实现负责更新内部缓存（带锁保护）并按需重启后台线程，例如 `FaultReporter.update_config` 会在 endpoints / pod_ip / ZMQ 端口变化时重建 ZMQ SUB（详见 `T5_02_EngineManager.md` 与 `T5_03_HeartbeatManager.md`）。

## 优雅停机与信号语义

`stop_all_modules()`（`main.py:79-87`）按 LIFO 顺序弹出并调用 `module.stop()`。`Daemon.stop` 会向所有 `engine_pids` 发 SIGKILL（`daemon.py:173-186`），保证子进程不会残留；HeartbeatManager 与 FaultReporter 通过 `stop_event` 通知后台线程退出并 `join`（`heartbeat_manager.py:121-127`、`fault_reporter.py:72-79`）。

`signal_handler` 与 `suicide_procedure` 的协作保证：

- 正常 SIGTERM：执行 `stop_all_modules`，main 返回 `-1`，K8s 据此重启 Pod。
- 心跳自杀：直接进入 `suicide_procedure` 立即停模块，避免在故障状态下持续处理请求。

## 文档交叉引用

- 引擎进程拉起细节：`T5_02_EngineManager.md`。
- 心跳机制：`T5_03_HeartbeatManager.md`。
- EngineServer 启动与 StartCmdMsg 解析：`T5_04_EngineServer启动流程.md`。
- 容器快照机制：`T5_06_Snapshot机制.md`。