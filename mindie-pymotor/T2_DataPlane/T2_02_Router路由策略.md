# T2_02_Router路由策略

Router 是 Coordinator 数据面的核心决策点：每个用户请求进入 Worker 进程后，`dispatch.handle_request`（`motor/coordinator/router/dispatch.py:150`）根据**当前实例拓扑**而不是静态配置选择 `BaseRouter` 子类，再交给子类完成 P/D 调度、HTTP 转发、流式回包。本文梳理 `BaseRouter` 抽象、两种实现（`UnifiedPDRouter`、`PDHybridRouter`）的差异，以及路由决策发生在哪一帧、依赖哪些上下文。

## BaseRouter 抽象

`BaseRouter`（`motor/coordinator/router/strategies/base.py:109`）是所有路由器的父类。它接收 `req_info`、`config`、`scheduler: SchedulingFacade`、`request_manager`、`sampling_manager` 五个依赖，封装与具体策略无关的通用能力：

- **生命周期管理** `_manage_request_context`（`base.py:210-222`）：在 Router 入口将 `RequestInfo` 注册到 `RequestManager`，退出时清理并打印状态日志。
- **资源管理** `_manage_resource_context`（`base.py:240-267`）：通过 `scheduler.select_and_allocate()` 做"选择实例 + 一次性预占 workload"，并在 `finally` 块里 `CancelScope(shield=True)` 包住 `release_func`，确保客户端断连也能完成 workload 释放。
- **HTTP 客户端管理** `_manage_client_context`（`base.py:223-238`）：从 `HTTPClientPool` 取出 endpoint 对应的长连接 client。
- **请求转发** `forward_request`/`forward_stream_request`（`base.py:397-548`）：构造 `X-Request-Id`、trace header、上游错误时构造 `UpstreamHTTPError`，并按 `infer_timeout` / `first_token_timeout` 控制超时。
- **trace span 管理** `_trace_span`（`base.py:192-204`）：开启 OTEL span，记录 `stream`、`requestId` 与错误信息。
- **Workload 释放** `release_all`/`release_tokens`/`release_kv`（`base.py:600-610`）：通过 `WorkloadActionHandler` 计算 workload 变化并调用 Scheduler 写回。

子类必须实现 `async def handle_request() -> Response`（`base.py:206-208`），决定走流式还是非流式、是否需要多 leg 协调。

## 路由选择：`select_router_class`

`select_router_class`（`motor/coordinator/router/dispatch.py:105-146`）是 Router 派发逻辑的核心。它**不读 deploy_mode 配置**，而是直接问 Scheduler 当前能看见什么：

1. 调用 `scheduler.get_available_instance_roles()` 拿到当前可用 role 集合；
2. 若 `ROLE_P` 与 `ROLE_D` 同时存在，再检查 `scheduler.has_compatible_pd_pair()`（`scheduler.py:219-223`）：当存在兼容的 dispatch 能力（即 `has_compatible_dispatch_pair` 返回 True）时返回 `UnifiedPDRouter`；
3. 仅当 P/D 存在但**不**兼容时，且没有 `ROLE_U`，直接抛 503，避免路由到一条注定失败的路径；
4. 退化分支：若 `ROLE_U` 或 `ROLE_P` 仍存在（即便 P/D 不兼容或只有 prefill），返回 `PDHybridRouter`，并在 `req_info.trace_obj` 上记录 meta warning 提示"虚推回退"——这是 A5 场景 D 实例虚推兼容的关键逻辑；
5. 完全无可用 role 时抛 503。

注意：`ROLE_P` 单独存在时也会退化到 `PDHybridRouter`（`dispatch.py:138-142`），并写入 `trace_obj.set_trace_error_message("PD separate service degraded to hybrid: only prefill instances available")`。这意味着 unified 路径总是优于 hybrid，统一 PD 在拓扑退化时才让位给 hybrid。

## 两种实现的差异

| 维度 | UnifiedPDRouter | PDHybridRouter |
|------|-----------------|----------------|
| 文件位置 | `motor/coordinator/router/strategies/unified_pd.py:63` | `motor/coordinator/router/strategies/pd_hybrid.py:38` |
| 适用拓扑 | 存在 P + D 且兼容 | 仅 U，或仅 P（D 不可用/虚推） |
| 工作模式 | 双 leg：P 跑 prefill，D 跑 decode | 单 leg：U/P 实例同时跑 prefill + decode |
| 流式分发 | `CommitAwareStreamingResponse` 控制 prefill + decode 两端都 ready 后再 commit HTTP 200（`unified_pd.py:139-145`） | 同样使用 `CommitAwareStreamingResponse`，但只需要 engine 端 ready（`pd_hybrid.py:157-163`） |
| 重试维度 | attempt 级别（`PDDispatchSession.new_attempt`） | request 级别（每次 attempt 重新做 select_and_allocate） |
| KV 亲和 | P 端可选 KV cache 亲和（见下文） | 不直接做 KV 亲和（union 实例无法精细命中） |

两种实现共享同一组通用能力（base class 提供），仅在 `_resolve_candidate_roles`、`_manage_resource_context`、`_run_*_attempt` 处分叉。

## 何时做路由决策

路由决策发生在请求处理的**最前沿**：

```
POST /v1/chat/completions
  └─ _handle_openai_request (inference_server.py:458)
      └─ dispatch.handle_request (dispatch.py:150)
          ├─ __create_request_info         # 解析 body、生成 req_id
          ├─ select_router_class           # 选 Router 子类
          └─ router_impl.handle_request()  # 子类各自走 P/D 或 hybrid 流程
```

`router_impl_class = await select_router_class(scheduler, req_info=req_info)`（`dispatch.py:181`）会在请求体解析完毕后立即执行；选错拓扑会立刻以 503 抛出，避免下游再做无意义的资源分配。

## KV Cache 亲和路由

当 `scheduler_type` 为 `KV_CACHE_AFFINITY` 且当前 role 为 `ROLE_P` 时，`KvCacheAffinityPolicy.select_instance_and_endpoint_from_list`（`motor/coordinator/scheduler/policy/kv_cache_affinity.py:409-422`）会先调用 `KvCacheAffinityPolicy.select_endpoint_from_list` 做基于 prefix 的候选排序，对其他 role 退回 `LoadBalancePolicy`。这一逻辑在 BaseRouter 调用 `select_and_allocate` 时由 Scheduler 自动应用，Router 本身不感知——只需拿到 `(instance, endpoint)` 即可。

## 跨文档引用

- `UnifiedPDRouter` 详细双 leg 流程见 `T2_02_1_统一PD路由逻辑.md`。
- `PDHybridRouter` 单实例流程见 `T2_02_2_混合部署路由逻辑.md`。
- `SchedulingFacade` 接口与 `select_and_allocate` 行为见 `T2_03_Scheduler调度器.md`。
- KV 亲和具体评分公式见 `T2_03_2_KVCache亲和性调度.md`。
- `CommitAwareStreamingResponse` 与 `StreamCommitController` 的 commit barrier 见 `T2_06_StreamResponse.md`。
