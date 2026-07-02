# T4_02 FaultManager

## 角色定位

FaultManager 是 Controller 侧的故障管理中枢，继承 `Observer`、`ThreadSafeSingleton` 以及 `_PersistenceMixin`、`_ResourceManagerMixin` 两个 mixin，集中提供三类能力：

- **故障检测与等级聚合**：把硬件（ConfigMap、Node）和软件（ZMQ PUB）来源的故障归并到实例级别。
- **隔离/恢复决策**：根据实例故障等级与 `InstanceManager` 协同实施强制隔离或解除隔离。
- **恢复策略调度**：在策略中心线程中按 fault_level 路由到具体的 `StrategyBase` 实现（如 `ScaleP2DStrategy`、`TokenReinferenceStrategy`）。

主体代码位于 `motor/controller/fault_tolerance/fault_manager.py`，并以 mixin 拆分：`_PersistenceMixin` 负责 ETCD 持久化（详见 T6 基础设施），`_ResourceManagerMixin` 负责节点同步与多实例节点映射。

## 核心数据结构

FaultManager 维护两个顶层映射（`motor/controller/fault_tolerance/fault_manager.py:68-69`）：

- `nodes: dict[str, NodeMetadata]`：以 K8s node_name 为键，保留硬件/软件故障历史。节点在实例移除时**不被删除**，以保证缩 P 保 D 场景下故障历史随节点转移到新实例时仍然可查。
- `instances: dict[int, InstanceMetadata]`：以 instance_id 为键，记录当前故障等级和正在运行的策略。

`ResourceMonitor` 字典（`resource_monitors`）以 node_name 为键维护每个节点的 K8s 监听器，由 `_create_resource_monitor_for_node` 创建并在节点删除时清理。

## 故障处理主链路

### 1. 实例生命周期接入

FaultManager 作为 `InstanceManager` 的观察者，`update()` 接收三种事件（`motor/controller/fault_tolerance/fault_manager.py:205-238`）：

- `INSTANCE_INITIAL`：调用 `_sync_instance_nodes` 将实例对应的 NodeManager 与 NodeMetadata 关联起来。
- `INSTANCE_SEPERATED`：实例被强制隔离后触发 `_refresh_instance_fault_level` 重评故障等级。
- `INSTANCE_REMOVED`：仅删除 `InstanceMetadata`，**保留** `NodeMetadata` 以备节点转给其他实例。

事件处理完成后通过 `work_condition.notify_all()` 唤醒策略中心线程，避免忙等。

### 2. 故障等级刷新

`_refresh_instance_fault_level`（`motor/controller/fault_tolerance/fault_manager.py:332-479`）的核心步骤：

1. 收集实例下所有节点的硬件/软件故障。
2. 对 `PreSeparateNPU` 做一次动态重评：有活跃实例则降为 `L2`，否则保持 `L6`。
3. 在 `instance_metadata.lock` 内聚合：取硬件与软件中 `fault_level` 最大的 `FaultInfo` 作为实例级别。
4. 根据最终等级调用 `InstanceManager.separate_instance()` 或 `recover_instance()`。

注意 `separate_instance` 只在 `fault_level > L2` 时触发，`≤ L2` 且实例已被隔离时则调用 `recover_instance`，与 `T4_01_故障分类体系.md` 中的等级语义一致。

### 3. 软件故障上报

`report_software_fault`（`motor/controller/fault_tolerance/fault_manager.py:282-330`）由 NodeManager 侧的 `FaultReporter` 调用，通过 `pod_ip` 找到对应 `NodeMetadata`，将 `FaultInfo` 写入 `software_fault_infos` 后刷新受影响实例的故障等级。

### 4. 策略中心

`_ft_strategy_center` 是后台线程主循环（`motor/controller/fault_tolerance/fault_manager.py:481-499`）：

- 遍历所有实例，调用 `_process_instance_strategy` 评估是否启动/停止/保持策略。
- 使用 `work_condition.wait(timeout=check_interval)` 实现事件驱动唤醒，取代固定 sleep 轮询。
- 通过 `stop_event` 优雅退出。

`_process_instance_strategy`（`motor/controller/fault_tolerance/fault_manager.py:501-599`）实现策略切换三态规则：

| 当前 vs 新等级 | 行为 |
|----------------|------|
| `current_strategy is None` | 启动新策略 |
| `new_level > current_level` | 升级：停止旧策略并启动新策略 |
| `new_level == current_level` | 保持：避免同级别抖动 |
| `new_level < current_level` | 降级：忽略，保留更高级别策略 |

策略在 `executor: ThreadPoolExecutor(max_workers=5)` 中异步执行；策略完成后 `_clear_software_faults` 清空软件故障并再次触发故障重评，等待下一次 ConfigMap 推送硬件故障状态变化。

## 持久化与配置热更新

- **ETCD 持久化**：`_PersistenceMixin` 在故障等级变更后调用 `persist_data()`，并在 `start()` 时尝试 `restore_data()` 恢复节点和实例数据（`motor/controller/fault_tolerance/fault_manager.py:122-125`）。
- **配置热更新**：`update_config()`（`motor/controller/fault_tolerance/fault_manager.py:157-203`）监听 `configmap_prefix`、`configmap_namespace` 变化，重建所有 `ResourceMonitor` 以应用新的 ConfigMap 命名规则。

## 与其他模块的协作

- **InstanceManager**：通过 `separate_instance`/`recover_instance` 控制实例状态机；通过 `is_instance_separated` 反查隔离状态。
- **ResourceMonitor**（`motor/controller/fault_tolerance/k8s/resource_monitor.py`）：每个节点一个，监听 Node 与 ConfigMap 变化，详见 `T4_04_K8s集成.md`。
- **StrategyBase** 子类：如 `ScaleP2DStrategy`（`T4_03_ScaleP2DStrategy.md`）和 `TokenReinferenceStrategy`。
- **Observability**：策略成功后通过 `add_alarm` 上报事件（见 `T4_03_ScaleP2DStrategy.md` 的 `_report_scale_p2d_event`）。

## 文档交叉引用

- 故障分类体系（等级、OriginFaultLevel、PreSeparateNPU）：`T4_01_故障分类体系.md`。
- 缩 P 保 D 策略的状态机与触发：`T4_03_ScaleP2DStrategy.md` / `T4_03_1_RecoveryState状态机.md` / `T4_03_2_P2D扩容触发条件.md`。
- ConfigMap 解析与 K8s 监听：`T4_04_K8s集成.md`。
- ETCD 持久化细节：T6 基础设施章节。