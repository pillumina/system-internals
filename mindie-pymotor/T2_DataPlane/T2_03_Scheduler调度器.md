# T2_03_Scheduler调度器

Scheduler 是 Coordinator 数据面"选择哪台实例、占用多少 workload"的决策点。它以两种形态存在：

- **同进程形态**：`Scheduler`（`motor/coordinator/scheduler/scheduler.py:41`），在 SchedulerServer 进程内被实例化一次，`SchedulerConnectionManager` 通过它完成 `select_and_allocate`/`update_workload` 等调用；
- **跨进程形态**：`AsyncSchedulerClient`，封装 ZMQ 客户端，由 Worker 进程通过 `SchedulerConnectionManager.get_client()` 拿到后注入到 `BaseRouter`。

两种形态实现 `SchedulingFacade` 协议（`motor/coordinator/domain/scheduling.py:116`），所以 Router 层不感知自己调用的是本地对象还是远程 stub。

## SchedulerServer 与 SchedulerConnectionManager

`SchedulerConnectionManager.from_config(config)`（在 `motor/coordinator/scheduler/runtime.py` 中定义）根据 `coordinator_config.scheduler_runtime` 决定启动本地进程还是连接远端：

- 同进程模式：直接在 Worker 进程内构造 `Scheduler` 实例，零 IPC 开销；
- 跨进程模式：拉起独立 SchedulerServer 进程，Worker 通过 ZMQ PUB/SUB 或 REQ/REP 与其通信。

不论哪种模式，`Mgmt` 与 `Inference` Server 都在构造时调用 `await self._scheduler_connection.connect()`（`management_server.py:131`、`inference_server.py:165`），生命周期结束时调用 `disconnect()` 关闭连接。

## SchedulingFacade 协议

`SchedulingFacade`（`scheduling.py:116-158`）定义了 Router 关心的全部调度原语：

```python
class SchedulingFacade(Protocol):
    async def select_and_allocate(self, role: PDRole, req_info: RequestInfo, *,
                                  target_instance_id: int | None = None
                                  ) -> tuple[Instance, Endpoint, Workload] | None: ...
    async def update_workload(self, params: UpdateWorkloadParams) -> bool: ...
    async def has_required_instances(self) -> InstanceReadiness: ...
    async def get_available_instances(self, role: PDRole | None = None) -> dict[int, Instance]: ...
    async def get_available_instance_roles(self) -> set[PDRole]: ...
    async def has_compatible_pd_pair(self) -> bool: ...
```

`select_and_allocate` 是 Router 唯一的资源分配入口，**原子地**完成"选实例 + 一次性预占 workload"，返回 `(instance, endpoint, workload)`。Router 用这个三元组构建 `ScheduledResource`（`scheduling.py:92`）并把它交给后续的 HTTP 转发逻辑。

`InstanceReadiness`（`scheduling.py:32-54`）是 readiness 的枚举：`REQUIRED_MET_EPD`/`REQUIRED_MET` 视为 ready（`is_ready()`），`ONLY_PREFILL`/`ENCODE_PREFILL` 视为可跑（`is_run()`）。`InferenceServer._is_available()`（`inference_server.py:343-351`）通过 `has_required_instances().is_run()` 决定入口是否接受新请求。

## Scheduler 主类

`Scheduler.__init__`（`scheduler.py:48-85`）核心步骤：

1. 根据 `config.scheduler_config.scheduler_type`（`SchedulerType` 枚举）通过 `SchedulingPolicyFactory.create` 实例化策略对象（`scheduler.py:74`），可注册 `KV_CACHE_AFFINITY`、`LOAD_BALANCE`、`ROUND_ROBIN`；
2. 若 `set_endpoint_instance_score_weight` 存在（`LoadBalancePolicy` 有），设置 `endpoint_instance_score_weight`；
3. 初始化精度采样的全局状态字典（per PD group 的 `_sample_exit_last_time`/`_precision_streak_counts`/`_precision_probing`/`_precision_action_tokens`），所有 Worker 共享。

`select_and_allocate`（`scheduler.py:110-152`）：

```python
async def select_and_allocate(self, role, req_info, *, target_instance_id=None):
    if target_instance_id is not None:
        pool = self._instance_provider.get_available_instances(role)
        instance = resolve_pinned_instance(pool, target_instance_id)
        ...
        endpoint = select_endpoint_for_instance(instance, scheduler_type=policy_type)
        ...
    else:
        r = self._scheduling_policy.select_instance_and_endpoint(role)
        result = (await r) if asyncio.iscoroutine(r) else r
        if result is None:
            return None
        instance, endpoint = result
    return await self._allocate_selected(instance, endpoint, role, req_info)
```

注意 `select_instance_and_endpoint` 在 policy 层可能是同步也可能是异步；KV 亲和策略实际是同步的（返回 `(instance, endpoint, score)`，score 透传给 scheduler 让 fresh load 再做一次仲裁）。

`_allocate_selected`（`scheduler.py:154-178`）计算 workload：

- 没有 `update_workload` 能力的策略（如 RoundRobinPolicy）→ `Workload()`（零预占）；
- 其他策略 → `calculate_demand_workload(role, req_info)`，基于 request 的 prompt token 数（来自 `req_info.token_ids`）估算 KV 预占。

最终通过 `update_workload(params)` 把 workload 写回 `InstanceProvider`。`InstanceProvider` 在 in-process 模式下直接调到 `InstanceManager.update_instance_workload`（详见 `T2_04_RequestManager.md`）。

## 调度策略工厂

`SchedulingPolicyFactory`（`motor/coordinator/scheduler/policy/factory.py:64`）是 OCP 友好的注册表：

```python
_REGISTRY: dict[SchedulerType, PolicyFactory] = {}

def register(scheduler_type, factory): _REGISTRY[scheduler_type] = factory
def create(scheduler_type, instance_provider):
    factory = _REGISTRY.get(scheduler_type)
    if factory is None:
        raise ValueError(f"Unsupported scheduling policy: {scheduler_type}")
    return factory(instance_provider)
```

`_register_builtin()`（`factory.py:73-78`）默认注册三种策略：`ROUND_ROBIN`、`LOAD_BALANCE`、`KV_CACHE_AFFINITY`。新增策略只需在外部模块 `register(MySchedulerType, my_factory)` 即可，无需修改 Scheduler。

详细策略行为见：
- `T2_03_1_调度策略工厂.md`
- `T2_03_2_KVCache亲和性调度.md`
- `T2_03_3_负载均衡策略.md`
- `T2_03_4_轮询策略.md`

## 实例视图

`Scheduler.get_available_instances`（`scheduler.py:195-200`）是 in-process 快路径，直接 `dict(...)` 包装 `InstanceProvider.get_available_instances(role)`，避免 `asyncio.to_thread`。

`has_compatible_pd_pair`（`scheduler.py:219-223`）检查当前 P/D 实例是否声明同一套 dispatch capability，调用 `has_compatible_dispatch_pair`（`motor/common/resources/dispatch.py`），被 `select_router_class` 用于 unified 路径判断。

## 精度采样协作

Scheduler 在全局维护 per-PD-group 的采样退出窗口：

- `confirm_sample_exit`（`scheduler.py:248-270`）：保证两个相邻 sample 至少间隔 `interval_seconds`，避免高频采样打爆引擎；
- `record_precision_result`（`scheduler.py:272-322`）：累加连续异常计数，达到 `threshold` 时发 `action_token` 让上层进入 probe；并发安全通过 `_sample_exit_lock(key)`（`scheduler.py:243-246`）；
- `finish_precision_action`（`scheduler.py:324-351`）：probe 完成后清状态并校验 token，防止过期 token 误清。

这部分只在 `precision_check_enabled=True` 时被 Worker 的 `SamplingManager` 调用，对正常路由透明。

## 跨文档引用

- `SchedulingFacade` 的字段语义见 `T2_02_Router路由策略.md`。
- `RequestManager` 与 `WorkloadActionHandler` 如何消费 `ScheduledResource` 见 `T2_04_RequestManager.md`。
- 调度策略选择与注册见 `T2_03_1_调度策略工厂.md`。
- Controller 通过 `ConductorApiClient` 投递 KV 实例名单（prefill/union）见 `T3_03_EventPusher.md`。
