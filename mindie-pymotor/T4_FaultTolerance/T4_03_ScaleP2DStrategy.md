# T4_03 ScaleP2D 策略

## 背景与目标

在 PD 分离（Prefill/Decode 解耦）部署下，每个 Decode 实例独占一部分 NPU 节点。当某个 Decode 实例出现 `L4–L6` 级硬件故障且集群内没有冗余节点直接替补时，Decode 侧推理能力将不可用。ScaleP2D（Scale Prefill to Decode）的核心思路是：**主动停止若干 Prefill 实例**，将其占用的节点释放给 Decode 实例恢复，从而在有限集群资源下最大化保障推理服务可用性。

策略实现集中在 `motor/controller/fault_tolerance/strategy/scale_p2d.py`，由 FaultManager 在策略中心线程池中异步执行（详见 `T4_02_FaultManager.md`）。

## 策略入口与路由

策略路由由 `motor/controller/fault_tolerance/strategy/strategy.py` 维护：

- `level4_strategy`（`strategy.py:71-83`）：当 `enable_scale_p2d == true` 且 `get_instance(instance_id).role == "decode"` 时返回 `ScaleP2DStrategy`。
- `level5_strategy`/`level6_strategy`（`strategy.py:86-93`）：当前实现委托给 `level4_strategy`，行为一致。

非 Decode 角色或开关关闭时，策略工厂返回 `None`，FaultManager 不会创建策略实例，但故障等级仍会驱动隔离动作（`> L2` → `separate_instance`）。

## 恢复流水线

`ScaleP2DStrategy.execute()`（`scale_p2d.py:82-161`）按以下顺序执行四步：

| 步骤 | 方法 | 关键输出 | 失败处理 |
|------|------|----------|----------|
| 1 | `_get_d_instance` | `num_required_node` = D 实例下 `L3+` 故障节点数 | `RecoveryState.FAILED` |
| 2 | `_check_d_instance_status` | 等待 D 进入 `INACTIVE`（轮询 3s 间隔，超时 `scale_p2d_d_instance_reinit_wait_timeout`） | `FAILED`，若 D 已恢复为 `INITIAL/ACTIVE` 则视为 `not needed` |
| 3 | `_select_p_instances_to_kill` | `selected_p_instances` | `FAILED`（容量不足或无可用 P） |
| 4 | `_kill_and_release_p_instances` | 对每个 P 实例的所有 NodeManager 调用 `NodeManagerApiClient.stop` | `FAILED`（部分 stop 失败即返回） |

四步均成功后将 `RecoveryState` 置为 `SUCCESS`，并触发 `_report_scale_p2d_event`（`scale_p2d.py:163-184`）上报 `ScaleP2DEvent` 到 `Observability`。

## 关键参数

| 参数 | 来源 | 默认/含义 |
|------|------|-----------|
| `CHECK_D_INSTANCE_STATUS_INTERVAL` | `scale_p2d.py:67` | 3 秒，D 实例状态轮询间隔 |
| `scale_p2d_d_instance_reinit_wait_timeout` | `ControllerConfig.fault_tolerance_config` | D 实例等待隔离的超时上限 |
| `enable_scale_p2d` | 配置开关 | `false` 时 L4+ 故障不触发 ScaleP2D |
| `num_required_node` | `_get_faulty_node_count`（`scale_p2d.py:265-312`） | D 实例下 `L3+` 故障节点数；元数据缺失时保守视为全部节点故障 |

## 容量校验与 P 选择

`_select_p_instances_to_kill`（`scale_p2d.py:435-534`）的容量计算：

```
num_available_node = num_node_per_instance_P × (operational_p_count - 1)
```

预留一个 Prefill 实例避免完全清空 P 池。可用容量 `< num_required_node` 时直接失败，不进入选择阶段。

`_select_instances_algorithm`（`scale_p2d.py:536-568`）当前为占位实现：按 `(status_priority, inst.id)` 排序，优先选择 `INITIAL` 状态、ID 较小的实例。该方法标记为可插拔，未来将替换为基于负载、优先级、运行时间的代价模型。

## 并发与线程安全

- `RecoveryContext` 仅在 `execute()` 所在线程内读写（`scale_p2d.py:69-71`），不需要外部加锁。
- `StrategyBase._lock` 保护 `_is_finished`；`stop()` 通过 `threading.Event` 通知 `_check_d_instance_status` 退出（`scale_p2d.py:633-642`）。
- FaultManager 通过 `InstanceMetadata.lock` 更新 `strategy` 引用，与策略线程解耦，避免与 `_refresh_instance_fault_level` 发生竞争（`fault_manager.py:501-599`）。

## 容错语义与日志关键字

策略的关键失败模式及其日志关键字（便于排障）：

| 场景 | 关键字 | 检查项 |
|------|--------|--------|
| D 实例不在 InstanceManager | `instance_not_in_instance_manager` | ETCD 同步完成、实例未被删除 |
| D 状态等待超时 | `D instance status check timed out` | 故障隔离已置 `INACTIVE`、InstanceManager 同步不延迟 |
| 容量不足 | `Insufficient Prefill nodes for ScaleP2D` | P 池规模、`num_required_node` |
| stop 失败 | `Failed to stop P instance node` | NodeManager 存活、网络可达、Pod 未删除 |

## 文档交叉引用

- 状态机各阶段详解：`T4_03_1_RecoveryState状态机.md`。
- 触发条件详述：`T4_03_2_P2D扩容触发条件.md`。
- 与 FaultManager 的协作时序：`T4_02_FaultManager.md` 中 `_process_instance_strategy` 与 `_ft_strategy_center`。
- 缩 P 保 D 在 PD 分离体系下的端到端作用：`docs/zh/design/fault_tolerance/scale_p2d.md`、`docs/zh/design/fault_tolerance/overview.md`。