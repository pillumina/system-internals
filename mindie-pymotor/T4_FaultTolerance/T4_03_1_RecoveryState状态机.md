# T4_03_1 RecoveryState 状态机

## 状态枚举

`RecoveryState` 定义在 `motor/controller/fault_tolerance/strategy/scale_p2d.py:26-34`，是 `str` 类型枚举，承载 ScaleP2D 单次恢复运行的语义状态：

```python
class RecoveryState(str, Enum):
    INIT = "init"
    CHECKING = "checking"
    SELECTING = "selecting"
    KILLING = "killing"
    SUCCESS = "success"
    FAILED = "failed"
```

前四个状态对应恢复流水线的四个执行阶段（与 `_execute_recovery_flow` 的步骤一一对应），后两个为终态。`RecoveryContext.current_state` 在每一步入口被覆盖写入（`scale_p2d.py:224, 324, 442, 572`），同时写入 `last_error` 字段记录失败原因，便于日志聚合与告警关联。

## 状态转移图

```text
                ┌─────────────────────────────────────────────┐
                │                                             │
                ▼                                             │
   ┌────────────────────┐    加载 D 成功    ┌──────────────────┴──┐
   │        INIT        │ ───────────────▶ │      CHECKING       │
   │ (entry, _get_d)    │                  │ (_check_d_instance) │
   └────────────────────┘                  └─────────┬───────────┘
                                                   │
                       D 已恢复 INITIAL/ACTIVE      │
                              ──────────────┐      │ D 进入 INACTIVE
                                            ▼      ▼
                                    ┌──────────────┐  资源不足
                                    │  not_needed  │  ─────────┐
                                    │   (early     │           │
                                    │   return)    │           ▼
                                    └──────────────┘  ┌──────────────────────┐
                                                     │      SELECTING       │
                                                     │(_select_p_to_kill)   │
                                                     └─────────┬────────────┘
                                                               │
                                                  容量校验失败 │
                                                  ─────────────┤
                                                               ▼
                                                    ┌──────────────────────┐
                                                    │       KILLING        │
                                                    │(_kill_and_release_p) │
                                                    └─────────┬────────────┘
                                                              │
                                                  stop 失败    │ 全部 stop 成功
                                                  ─────────────┤
                                                               ▼
                                       ┌──────────────┐    ┌──────────────┐
                                       │    FAILED    │    │   SUCCESS    │
                                       │   (终态)      │    │   (终态)     │
                                       └──────────────┘    └──────────────┘
```

## 阶段详解

### INIT

由 `_get_d_instance`（`scale_p2d.py:222-263`）设置。读取 `InstanceManager.get_instance(d_instance_id)`，并通过 `len(d_instance.get_node_managers())` 计算 `num_node_per_instance_D`。`num_required_node` 由 `_get_faulty_node_count`（`scale_p2d.py:265-312`）统计 `L3+` 故障节点数得到。失败可能原因：实例未注册到 InstanceManager、K8s 元数据缺失。

### CHECKING

由 `_check_d_instance_status`（`scale_p2d.py:314-433`）设置。该阶段是 ScaleP2D 的**安全门**：必须确认 D 实例已经被 `InstanceManager.separate_instance` 置为 `INACTIVE`，否则可能误杀 Prefill 实例影响线上业务。

关键逻辑：

- 通过 `InstanceManager.get_instance_by_job_name`（而非 `get_instance(id)`）轮询，因为冗余节点恢复可能复用 `job_name` 并分配新 `id`。
- 轮询间隔 `CHECK_D_INSTANCE_STATUS_INTERVAL = 3` 秒（`scale_p2d.py:67`）。
- 超时上限 `scale_p2d_d_instance_reinit_wait_timeout`（默认从配置读取）。
- 若轮询期间发现 D 已回到 `INITIAL/ACTIVE`，视为 `not needed`：故障自愈，ScaleP2D 无需执行，立即返回。

### SELECTING

由 `_select_p_instances_to_kill`（`scale_p2d.py:435-534`）设置。容量计算：

```python
num_available_node = num_node_per_instance_P * (len(operational_p_instances) - 1)
```

`operational_p_instances` 仅包含 `InsStatus.INITIAL/ACTIVE` 的 Prefill 实例，`INACTIVE/DELETED` 等待 InstanceManager 清理。容量不足时直接失败；通过容量校验后调用可插拔的 `_select_instances_algorithm`。

### KILLING

由 `_kill_and_release_p_instances`（`scale_p2d.py:570-631`）设置。串行遍历每个被选 Prefill 实例，对其中的每个 NodeManager 调用 `NodeManagerApiClient.stop`。任一 `stop` 失败即返回 `False`，整体状态记为 `FAILED`。

### SUCCESS / FAILED

终态。在 `execute()` 的 `finally` 块统一处理（`scale_p2d.py:142-161`）：

- 计算 `elapsed_s`，记录到日志。
- 仅当 `SUCCESS` 时调用 `_report_scale_p2d_event` 上报 `ScaleP2DEvent`，由 `Observability.add_alarm` 写入告警通道。
- 不论结果如何，最终都会将 `_is_finished` 置 `True`，FaultManager 在下一轮策略中心循环中识别后清空 `strategy` 引用并触发故障重评。

## 状态转移与异常路径

`execute()` 的 try/except 结构（`scale_p2d.py:89-141`）保证：即便任何阶段抛出未捕获异常，`RecoveryState` 也会被置为 `FAILED`，`last_error` 写入异常字符串。这避免了策略长时间停留在中间态。

`stop()`（`scale_p2d.py:633-642`）通过 `threading.Event.set()` 通知 `_check_d_instance_status` 提前返回 False，此时 `RecoveryState` 取决于返回时机的状态：若处于 CHECKING 阶段被中断，会被 `execute()` 的失败分支写为 `FAILED`；若已完成 KILLING，状态保持原值。

## 与 FaultManager 的协作

FaultManager 的 `_process_instance_strategy`（`fault_manager.py:501-599`）以轮询方式观察策略完成：

1. 策略进入 `executor.submit(...)` 后立即返回，FaultManager 不阻塞。
2. 每轮策略中心循环检查 `is_finished()`，若完成则清空 `ins_metadata.strategy` 并重置 `strategy_fault_level`。
3. 完成后 FaultManager 调用 `_clear_software_faults` 清空软件故障，再次触发 `_refresh_instance_fault_level` 让 ConfigMap 推送的硬件故障重新决策。

该闭环保证 ScaleP2D 完成一次后，故障等级重新评估为 HEALTHY，实例可正常恢复 ACTIVE 状态。

## 文档交叉引用

- 策略入口与四步流水线概述：`T4_03_ScaleP2DStrategy.md`。
- 触发条件（角色、故障等级、开关）：`T4_03_2_P2D扩容触发条件.md`。
- 与策略中心的协作：`T4_02_FaultManager.md`。