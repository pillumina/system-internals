# T4_05 PrecisionAlarm

## 背景

推理精度异常（Precision Issue）是 PD 分离部署中的一种隐蔽故障：模型输出与预期分布出现偏差（错字、重复、幻觉等），但 HTTP 响应仍是 200 OK，传统健康检查无法捕获。Coordinator 侧通过 `ChatProbe` 周期性巡检 P/D 推理结果，调用 msprobe `ILLDetector` 检测 token 级异常，连续命中后通过 `PrecisionAlarm` 向 Controller 上报，由控制面触发隔离与恢复流程。

涉及代码：

- `motor/coordinator/fault_tolerance/precision/checker.py`：msprobe 封装与 `CheckResult` 数据结构。
- `motor/coordinator/fault_tolerance/alarm/precision_alarm.py`：`AlarmAction` 子类，串联 probe 与上报。
- `motor/coordinator/fault_tolerance/alarm/base.py`：`AlarmAction` 抽象基类与 `AlarmContext` 数据载体。
- `motor/common/alarm/precision_issue_alarm.py`：上报 payload 的构造。

## 检测算法抽象

`PrecisionChecker`（`checker.py:64-74`）是检测器抽象：

```python
async def check(
    self,
    prompt_token_ids: list[int],
    output_token_ids: list[int],
    logprobs: list[float],
    *,
    topk_logprobs: list[dict[int, float]] | None = None,
    model: str | None = None,
) -> CheckResult
```

`CheckResult`（`checker.py:56-61`）包含 `has_issue`、`issue_type`、`confidence`、`detail`。

## MsprobeChecker

`MsprobeChecker`（`checker.py:130-212`）是生产实现，基于 msprobe `ILLDetector`。关键设计点：

- **延迟加载单例**：`ILLDetector` 通过 `_get_msprobe_detector`（`checker.py:94-121`）懒加载，避免导入 msprobe 成为硬依赖；同时解析 msprobe 包内 `configs/config.yaml`、`configs/mtype_config.json`、`token2category/` 路径，使检测器不依赖当前工作目录。
- **线程安全**：msprobe 的 `ILLDetector` 内部状态（`_garbled_count`、`topk` 等）非线程安全；`MsprobeChecker.check` 在 `_msprobe_lock`（`checker.py:38`）内串行调用 `detector.run`，并通过 `asyncio.to_thread` 移出事件循环（`checker.py:178-188`）。
- **fail-open 语义**：缺少 `topk_logprobs`、长度不一致或 `detector.run` 异常时，返回 `has_issue=False`，**不阻断**推理；这避免了检测器异常扩散为业务中断。`_normalize_ill_type`（`checker.py:41-53`）对 msprobe 历史上将 `ill_type` 默认值设为 `int` 类型对象（`type` 实例）的 bug 做兼容，将其归一化为 `0`。
- **topk 降级**：当上游未提供 `topk_logprobs` 时，`_build_topk_fallback`（`checker.py:77-91`）用 `(token_id → logprob)` 单键字典构造，长度不一致则放弃。

## ChatProbe 与 AlarmAction 串联

`motor/coordinator/fault_tolerance/probe/chat_probe.py` 提供 `ChatProbe.run(p_instance_id, d_instance_id, model, max_attempts, timeout_seconds)`，按 `(P, D)` 组实际发起若干次 chat 推理，调用 `MsprobeChecker.check` 累计失败次数。

`PrecisionAlarm`（`precision_alarm.py:24-74`）实现 `AlarmAction.execute(ctx)`：

1. 从 `ctx.extra` 取出 `model` 名。
2. 调用 `ChatProbe.run` 收集 `outcome.failures`。
3. 通过 `build_precision_issue_alarm`（位于 `motor/common/alarm/precision_issue_alarm.py`）构造上报 payload，包含 `p_instance_id`、`d_instance_id`、`precision_issue_count`、`probe_failure_count`、`model_id`。
4. 调用 `ControllerApiClient.report_alarms(payload)`，由 Controller 侧进入告警聚合与隔离决策。

`AlarmAction` 基类与 `AlarmContext`（`alarm/base.py:20-31`）定义了 `p_instance_id`、`d_instance_id`、`issue_count`、`extra` 等字段，便于在路由层按 PD 组复用同一执行流水线。

## 触发与聚合

Coordinator 通过路由器侧或后台采样器在发现连续若干次精度异常后构造 `AlarmContext`，注入 `issue_count`（业务侧累计的精度异常计数）和 `extra.model`。`PrecisionAlarm` 在此基础上再发起 ChatProbe 进行二次验证，并把验证后的 `probe_failure_count` 一并上报。Controller 在 `Observability` 中聚合来自不同 PD 组的告警，并按需触发实例隔离或恢复动作（详见 T4_02_FaultManager.md 与 T8_DeepDives 中的告警体系章节）。

## 文档交叉引用

- 抽象基类与 `AlarmContext` 定义：`motor/coordinator/fault_tolerance/alarm/base.py`，对应章节见 T3_ControlPlane 中的告警聚合。
- FaultManager 如何使用上报结果：`T4_02_FaultManager.md`。
- 集群连接与服务器异常的姊妹告警：`motor/common/alarm/cluster_connection_alarm.py`、`motor/common/alarm/instance_exception_alarm.py`。