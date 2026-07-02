# T2_06_StreamResponse

流式输出是 LLM 推理的常见形态——客户端拿到第一个 token 后需要持续接收后续 token。Coordinator 的 Router 把上游引擎的 SSE 流拆成 chunk，转发给客户端；但流式响应里有两个特殊问题：

1. **Commit barrier**：HTTP 响应头（状态码 200）什么时候发？必须在所有上游 leg 都"确认能跑"之后才能发，否则客户端会在已经看到 200 后才收到 502；
2. **断连传播**：客户端断开后必须立即取消上游引擎的生成请求，避免白白消耗算力。

`StreamCommitController` 与 `CommitAwareStreamingResponse`（`motor/coordinator/router/stream_response.py`）就是为这两个问题设计的，本文梳理它们的契约、commit barrier 的状态机以及与 `PDDispatchSession` 的协作。

## StreamCommitController

`StreamCommitController`（`stream_response.py:34-94`）是 commit barrier 的状态机：

```python
@dataclass
class StreamCommitController:
    required_parts: frozenset[str]
    _attempt_id: int = 0
    _ready_parts: set[str] = field(default_factory=set)
    _ready_event: asyncio.Event = field(default_factory=asyncio.Event)
    _committed_event: asyncio.Event = field(default_factory=asyncio.Event)
    _commit_started: bool = False
    _committed: bool = False
```

构造方式必须是 `StreamCommitController.requiring({"prefill", "decode"})`（unified）或 `StreamCommitController.requiring({"engine"})`（hybrid）——空集合会抛 `ValueError`（`stream_response.py:46-51`）。

关键方法：

- `begin_attempt(attempt_id)`（`stream_response.py:53-59`）：每次新 attempt 前重置 `_ready_parts`/`_ready_event`/`_committed_event`，但 `_committed` 一旦为 True（`commit_sealed`）就拒绝重启 attempt；
- `mark_ready(part, attempt_id)`（`stream_response.py:61-68`）：上游 leg 准备好后调用，`attempt_id` 不匹配或已 commit 的标记会被忽略；`required_parts.issubset(_ready_parts)` 触发 `_ready_event.set()`；
- `wait_ready()` / `wait_committed()`：分别等 ready 与 committed 信号；
- `commit_sealed`（`stream_response.py:81-82`）：`ready_to_commit or _commit_started or _committed`——一旦 ready，commit 边界就"封口"，后续 mark_ready / begin_attempt 都会被拒。

UnifiedPDRouter 与 PDHybridRouter 在入口创建：

```python
# unified_pd.py:139
self._stream_commit_controller = StreamCommitController.requiring({"prefill", "decode"})
# pd_hybrid.py:158
self._stream_commit_controller = StreamCommitController.requiring({"engine"})
```

## CommitAwareStreamingResponse

`CommitAwareStreamingResponse`（`stream_response.py:97-381`）继承自 FastAPI 的 `Response`，是 Router 实际返回给 FastAPI 的对象。它通过 ASGI 协议直接与客户端通信，而不是通过 Starlette 的 `StreamingResponse` 中间层——这让它可以**控制 HTTP 200 的发送时机**。

### ASGI 调用入口

`__call__(scope, receive, send)`（`stream_response.py:132-148`）：

1. 启动两个 task：`_stream_response`（真正发送响应）与 `_listen_for_disconnect`（监听客户端断开）；
2. 用 `asyncio.wait(..., return_when=FIRST_COMPLETED)` 谁先结束就取消另一个；
3. 客户端断开时以 `CLIENT_DISCONNECT` 原因取消 stream task。

### 内部状态机：`_stream_response`

`_stream_response`（`stream_response.py:153-198`）：

1. 创建 `terminal: Future` 与两个 task：`pump_task = _pump_stream(send, terminal)`、`ready_task = controller.wait_ready()`；
2. 等待 `terminal` 或 `ready_task` 完成：
   - 若 `terminal` 先完成但 `controller.ready_to_commit` 为 False：视为 ready 之前就出错，要么抛错要么 `RuntimeError("Streaming request ended before the commit barrier was satisfied")`；
   - 若 `ready_task` 先完成：调用 `controller.mark_commit_started()`、`send({"type": "http.response.start", "status": 200, "headers": raw_headers})`、`controller.mark_committed()`，然后 `await pump_task` 转发 chunk；
3. 任何异常分支根据 `controller.committed` 走 `_send_committed_error`（在 SSE 流里塞一个 `data: {"error": ...}` chunk）或 `_send_precommit_error`（重新构造 HTTP 响应，覆盖之前未发送的 200）。

`CommitAwareStreamingResponse` 把"ready barrier"作为**逻辑 commit 边界**——即便 stream task 比 ASGI 调度的 send 更早完成，也必须等 ready barrier 才能发出 HTTP 200（`stream_response.py:163-170`）。

### `_pump_stream`

`_pump_stream(send, terminal)`（`stream_response.py:200-228`）是 chunk 转发循环：

```python
async def _pump_stream(self, send, terminal):
    try:
        async for item in self._raw_iterator:
            await self.controller.wait_committed()
            await self._send_body(send, item, more_body=True)
            self._mark_first_body_sent()
    except asyncio.CancelledError:
        raise
    except OSError as error:
        raise ClientDisconnect() from error
    except Exception as error:
        if self.controller.commit_sealed:
            await self.controller.wait_committed()
            await self._send_committed_error(send, error)
        elif not terminal.done():
            terminal.set_result(error)
    else:
        if self.controller.commit_sealed:
            await self.controller.wait_committed()
            await self._finish(send)
        elif not terminal.done():
            terminal.set_result(None)
    finally:
        with CancelScope(shield=True):
            await self._close_iterator()
```

`wait_committed()` 等待 ready→committed 的过渡完成，确保 HTTP 200 一定先于第一个 body chunk 发出。

错误分支区分 `commit_sealed`（已经向客户端发过 200）与其他情况（precommit）：

- `commit_sealed` 后：把错误包成 SSE 错误 chunk 发送，模拟 OpenAI 错误格式；
- precommit：把错误存入 `terminal`，让 `_stream_response` 用 `_send_precommit_error` 重新构造 HTTP 响应。

### 错误体格式

`_committed_error_chunk`（`stream_response.py:278-311`）生成 `data: {"error": {...}}\n\n` SSE 错误。优先级：

1. `UpstreamHTTPError` 且 body 可解析为 JSON：原样转发引擎的错误 body（保留引擎侧语义）；
2. `UpstreamHTTPError` 但 body 不可解析：用 `error.status_code`；
3. `httpx.TimeoutException`：504；
4. `httpx.RequestError`：502；
5. `HTTPException` / `httpx.HTTPStatusError`：透传 code；
6. 其他：500。

### 单元测试兼容

`__init__`（`stream_response.py:102-131`）把 `body_iterator` 设置为 `_compatibility_iterator()`，让单元测试能直接迭代 `for chunk in response.body_iterator` 而不是走 ASGI。`_claim_consumer`（`stream_response.py:343-349`）保证 ASGI 与 body_iterator 互斥——一旦一方声明消费，另一方再调用就抛 `RuntimeError`。

## PDDispatchSession 与 AttemptContext

`PDDispatchSession`（`dispatch_session.py:184`）是 UnifiedPDRouter 的 attempt 编排上下文：

```python
class PDDispatchSession:
    def __init__(self, root_request_id: str) -> None:
        self.root_request_id = root_request_id
        self._attempt_seq = 0
        self.attempts: dict[int, AttemptContext] = {}

    def new_attempt(self, prefill_resource, decode_resource, config) -> AttemptContext:
        self._attempt_seq += 1
        attempt = AttemptContext(
            root_request_id=self.root_request_id,
            attempt_seq=self._attempt_seq,
            pair_id=uuid.uuid4().hex,
            prefill_resource=prefill_resource,
            decode_resource=decode_resource,
            config=config,
        )
        self.attempts[attempt.attempt_seq] = attempt
        return attempt
```

`AttemptContext`（`dispatch_session.py:47`）持有 `prefill_resource`/`decode_resource`/`state`/`release_flags`/`stop_lock`/`prefill_task`/`decode_task` 等。`state: AttemptState` 枚举了 `CREATED → DISPATCHING → ACTIVE → FIRST_VISIBLE → DONE`，外加 `STOPPING`/`STOPPED`。`transition(state)`（`dispatch_session.py:66-81`）实现严格状态机：`STOPPED` 之后只能停在 `STOPPED`，`STOPPING` 之后只能停 `STOPPED`，`DONE` 之后只能停 `DONE`。

`dispatch_for(role, dispatch_mode)`（`dispatch_session.py:89-101`）把 AttemptContext 转成 `MotorDispatch` envelope：

```python
return MotorDispatch(
    root_request_id=self.root_request_id,
    engine_request_id=f"{self.root_request_id}#a{self.attempt_seq}",
    pair_id=self.pair_id,
    attempt_seq=self.attempt_seq,
    role="prefill" if role == PDRole.ROLE_P else "decode",
    dispatch_mode=dispatch_mode,
    endpoints=DispatchEndpoints(
        prefill=_dispatch_endpoint(self.prefill_resource),
        decode=_dispatch_endpoint(self.decode_resource),
    ),
)
```

`_dispatch_endpoint`（`dispatch_session.py:209-217`）从 `ScheduledResource` 抽 `(instance_id, endpoint_id, url)`，供 EngineServer 端 `DispatchAdapter.adapt_request_body` 解析。

`register_canceller`/`unregister_canceller`（`dispatch_session.py:131-181`）把 `AttemptContext.cancel` 挂到 `HTTPClientPool` 的 canceller 注册表上，节点故障时通过 canceller 立即取消 in-flight 请求。

## mark_ready 的调用点

UnifiedPDRouter 中：

- `_run_concurrent_stream_attempt.prefill_task`（`unified_pd.py:352-353`）：`self._stream_commit_controller.mark_ready("prefill", attempt.attempt_seq)` 在 prefill HTTP 响应到达后调用；
- `_start_stream_decode_task` 的 `on_response_ready` lambda（`unified_pd.py:420`）：`self._stream_commit_controller.mark_ready("decode", attempt.attempt_seq)` 在 decode 收到 HTTP 200 后调用；
- `_request_prefill_result`（`unified_pd.py:687-691`）：handoff 路径下 prefill 返回 `PrefillResult` 后调用 `mark_ready("prefill", ...)`。

当 prefill 与 decode 都 mark_ready 后，`StreamCommitController._ready_event.set()` 触发，`_stream_response` 才能 send HTTP 200。

## 客户端断连传播

UnifiedPDRouter 在 `_pump_stream` 检测到 `ClientDisconnect` 后，会让 `_stream_response` 取消 stream_task。`AttemptContext.cancel(reason)`（`dispatch_session.py:111-129`）取消 prefill_task 与 decode_task，调用方在 `finally` 里跑 `_stop_attempt` 发 `/v1/dispatch/stop` 给两端引擎（`unified_pd.py:786-806`），实现"客户端断了引擎也立即停"。

PDHybridRouter 的断连传播更直接：`_manage_canceller_context`（`pd_hybrid.py:107-131`）注册了 node-fault canceller，HTTPClientPool 探测到 endpoint 失效时通过 canceller 立即取消 task，绕过 transport 超时。

## 跨文档引用

- `_run_concurrent_stream_attempt` 与 `_run_handoff_stream_attempt` 何时 mark_ready 见 `T2_02_1_统一PD路由逻辑.md`。
- EngineServer 端 `/v1/dispatch/stop` 与 `DispatchAdapter.stop_peer` 见 `T2_05_DispatchAdapter.md`。
- `_manage_canceller_context` 在 hybrid 下的应用见 `T2_02_2_混合部署路由逻辑.md`。
- `Rescheduler` 如何利用 `return_token_ids` 缓存的 token id 续跑见 `T2_02_2_混合部署路由逻辑.md`。
