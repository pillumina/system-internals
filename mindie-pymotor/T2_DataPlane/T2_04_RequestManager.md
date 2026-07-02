# T2_04_RequestManager

`RequestManager`（`motor/coordinator/domain/request_manager.py:23`）是 Coordinator 数据面的"请求账本"。它跟踪每个请求的状态、workload 配额和 attempt 元数据，被 Router、Scheduler、WorkloadActionHandler 共同读写。本文梳理请求 ID 生成、workload 字典语义、CancelScope 与超时控制的协作方式，以及错误路径下的清理闭环。

## 实例与并发模型

`RequestManager` 是普通类（非 singleton），每个进程（Mgmt / Inference Worker / Scheduler）持有一个实例。`__init__`（`request_manager.py:28-46`）初始化：

- `_rate_limit_config`：从 config 拿，便于运行时热更新（`update_config`，`request_manager.py:184-188`）；
- `_lock = asyncio.Lock()`：核心数据结构的互斥锁——hot path 上的增删改查都走它，**不阻塞 event loop**；
- `_config_lock = threading.RLock()`：仅在 `update_config` 时使用；
- `_req_info_dict: dict[str, RequestInfo]`：请求 ID → RequestInfo 映射；
- `_req_workload_dict: dict[tuple, Workload]`：workload 账本，key 见下文。

注意 `threading.Lock` 已被替换为 `asyncio.Lock`——这是为了让高 QPS 下 hot path 不卡住 event loop。`update_config` 仍用 `threading.RLock`，因为它从 FastAPI lifespan / config reload 触发，不在请求热路径上。

## 请求 ID 生成

`generate_request_id`（`request_manager.py:48-68`）格式：`{timestamp(16 位)}{counter(4 位)}{uuid_hex(8 位)}`，示例：`17000000000000000001234abcd1234`。ID 格式与生成规则：

```python
async def generate_request_id(self) -> str:
    async with self._lock:
        current_timestamp = int(time.time() * 1000000)  # 微秒
        if current_timestamp == self._last_timestamp:
            self._counter += 1
        else:
            self._counter = 0
            self._last_timestamp = current_timestamp
        counter_part = f"{self._counter:04d}"
    random_suffix = uuid.uuid4().hex[:8]
    return f"{current_timestamp}{counter_part}{random_suffix}"
```

单调递增计数器 + 微秒时间戳保证同进程内 ID 全局唯一且大致按时间排序；末尾 8 位 UUID 防止跨进程冲突。这一 ID 是后续在 Router / Scheduler / RequestInfo 上唯一的索引键。失败时退回纯 UUID（`request_manager.py:67`），保证 ID 一定可用。

## Workload 字典语义

`add_req_workload`/`get_req_workload`/`update_req_workload`/`del_req_workload`（`request_manager.py:112-182`）是 workload 账本的核心 API。`_workload_key`（`request_manager.py:107-110`）：

```python
@staticmethod
def _workload_key(req_id, role, attempt_seq=None):
    if attempt_seq is None:
        return (req_id, role)
    return (req_id, attempt_seq, role)
```

两种 key 形态：

- `(req_id, role)`：legacy router（`PDHybridRouter`、`BaseRouter._manage_resource_context`）使用；
- `(req_id, attempt_seq, role)`：Unified PD dispatch 使用（`UnifiedPDRouter._create_attempt` → `_record_attempt_workload`），保证 attempt 1 的延迟 cleanup 不会误释放 attempt 2 的 workload。

`add_req_attempt_workload` 在键已存在时返回 False，BaseRouter 据此调用 `_rollback_allocated_workload` 把 Scheduler 端的预占撤掉（`base.py:307-316`），形成"请求级 bookkeeping 失败 → 自动回滚"的闭环。

`del_req_info`（`request_manager.py:88-102`）在删除 RequestInfo 时同步清理该 req_id 下的所有 workload 记录：

```python
keys_to_delete = [k for k in self._req_workload_dict if k[0] == req_id]
for k in keys_to_delete:
    del self._req_workload_dict[k]
```

`k[0] == req_id` 同时匹配 `(req_id, role)` 和 `(req_id, attempt_seq, role)` 两种 key 形态——这是 RequestManager 的隐含约定。

## 请求生命周期

`BaseRouter._manage_request_context`（`motor/coordinator/router/strategies/base.py:210-222`）是 RequestManager 在 Router 层的标准用法：

```python
@contextlib.asynccontextmanager
async def _manage_request_context(self):
    await self._request_manager.add_req_info(self.req_info)
    try:
        yield
    finally:
        await self._request_manager.del_req_info(self.req_info.req_id)
        self._log_request_details()
```

进入上下文时 `add_req_info` 注册，退出时 `del_req_info` 同步清理。`del_req_info` 在 ID 不存在时返回 False 但不抛错，避免重复进入上下文造成故障。

## CancelScope 与超时

`RequestInfo.set_cancel_scope`/`cancel_scope`（位于 `motor/coordinator/models/request.py`）让 Router 把每个 role 的取消令牌挂到 RequestInfo 上，由 RequestManager 在请求结束时统一触发。

`BaseRouter._update_workload`（`base.py:701-723`）在释放阶段用 `with CancelScope(shield=True):` 包裹 Scheduler 调用，确保客户端断开也不会让 release RPC 失败：

```python
async def _update_workload(self, resource, action):
    workload_change, role = await self._workload_action_handler.compute_and_update(
        resource, self.req_info.req_id, action, self.req_info,
    )
    ...
    with CancelScope(shield=True):
        return await self._scheduler.update_workload(params)
```

这是 Coordinator 数据面"必须完成资源清理"的关键设计：所有 `release_func`（`_manage_resource_context` 的 `finally` 块、`_release_attempt` 等）都包在 `CancelScope(shield=True)` 里。

`_rollback_allocated_workload`（`base.py:366-395`）是反向操作：当 `add_req_workload` 发现 key 已存在（说明这次 scheduling 是个重复分配），立刻把已分配的 workload 撤掉。`active_kv_cache`/`active_tokens` 取负值后通过 `RELEASE_TOKENS` action 写回。

## WorkloadActionHandler

`WorkloadActionHandler`（`motor/coordinator/router/workload.py`）封装"根据 req_info 计算要释放的 workload 增量"。`BaseRouter._workload_action_handler`（`base.py:130-134`）默认实例化为 `WorkloadActionHandler(self._request_manager)`。它的 `compute_and_update` 同步更新 RequestManager 里的 workload 记录，再返回 `(workload_change, role)` 让 Router 把这个增量交给 Scheduler。

`UnifiedPDRouter._release_attempt_resource`（`unified_pd.py:754-784`）按 `(role, action)` 严格幂等，通过 `AttemptReleaseFlags`（`dispatch_session.py:39`）确保 `RELEASE_TOKENS` 与 `RELEASE_KV` 不会重复调用。

## 错误路径下的清理

`BaseRouter._manage_resource_context` 的 `finally`（`base.py:252-267`）：

```python
finally:
    if resource:
        if asyncio.iscoroutinefunction(release_func):
            with CancelScope(shield=True):
                result = await release_func(resource)
        else:
            result = release_func(resource)
        if not result:
            self.logger.debug("release_func(%s) returned False instance_id=%s ...",
                              role.name, resource.instance.id, resource.endpoint.id,
                              self.req_info.state)
```

任何路径下（异常、取消、正常完成）都会触发 `release_func`，release 失败仅记 debug，不向上抛——避免掩盖原始异常。

## 状态机

`ReqState`（`motor/coordinator/models/request.py`）枚举了 `ARRIVE → E_SCHEDULING → E_ALLOCATED → P_SCHEDULING → P_ALLOCATED → PREFILL_END → D_SCHEDULING → D_ALLOCATED → FIRST_TOKEN_FINISH → DECODE_END → DONE/EXCEPTION`。`req_info.update_state()` 在 `RequestManager` 之外，但 RequestManager 通过 `_req_info_dict` 持有 RequestInfo 引用，任何子模块都能 `request_manager.get_req_info(req_id).state` 读到当前状态。

## 跨文档引用

- `SchedulingFacade.select_and_allocate` 与 workload 写回路径见 `T2_03_Scheduler调度器.md`。
- Router `_manage_request_context` 与 `_manage_resource_context` 配合方式见 `T2_02_Router路由策略.md`。
- `PDDispatchSession` 的 `(req_id, attempt_seq, role)` key 见 `T2_02_1_统一PD路由逻辑.md`。
- 实例 readiness 与可运行性判断见 `T2_03_Scheduler调度器.md`。
