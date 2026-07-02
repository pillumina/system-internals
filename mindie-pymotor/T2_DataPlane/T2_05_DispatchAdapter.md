# T2_05_DispatchAdapter

`DispatchAdapter`（`motor/engine_server/core/dispatch_adapter/base.py:292`）是 EngineServer 侧的引擎协议归一化层。Coordinator 的 `UnifiedPDRouter` 在请求里塞入一个 `MOTOR_DISPATCH_KEY` envelope（详见 `T2_02_1_统一PD路由逻辑.md`），到达 EngineServer 后由 `DispatchAdapter.adapt_request_body` 拆开并翻译成当前引擎（vLLM / SGLang）能识别的字段。本文梳理 `DispatchAdapter` 的契约、vLLM 与 SGLang 两种实现的差异，以及 prefill result 的往返路径。

## 入口与请求翻译

`DispatchAdapter.adapt_request_body`（`base.py:307-336`）：

```python
async def adapt_request_body(self, body: dict[str, Any]) -> tuple[dict[str, Any], MotorDispatch | None]:
    dispatch_data = body.get(MOTOR_DISPATCH_KEY)
    if dispatch_data is None:
        return body, None
    self._reject_legacy_dispatch_fields(body)
    try:
        dispatch = MotorDispatch.model_validate(dispatch_data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid {MOTOR_DISPATCH_KEY}: {e}") from e
    self._validate_role(dispatch)
    await self._registry.activate(dispatch)
    try:
        engine_body = body.copy()
        engine_body.pop(MOTOR_DISPATCH_KEY, None)
        prefill_result = self._pop_and_validate_prefill_result(engine_body, dispatch)
        if prefill_result is not None:
            engine_body = await self._consume_prefill_result(engine_body, dispatch, prefill_result)
        engine_body = await self._adapt_engine_body(engine_body, dispatch)
        return engine_body, dispatch
    except Exception:
        await self.stop_peer(dispatch)
        await self._registry.finish(dispatch)
        raise
```

关键步骤：

1. 若 body 没有 dispatch envelope，按非 PD 请求原样放行；
2. `_reject_legacy_dispatch_fields`（`base.py:444-454`）显式拒绝 `kv_transfer_params` 与 `bootstrap_*` 等老协议字段，避免新旧混用；
3. `MotorDispatch.model_validate` 反序列化为 Pydantic 对象；
4. `_validate_role`（`base.py:435-442`）检查本地 endpoint role（`union/both` 跳过）与 dispatch role 是否一致，否则 400；
5. `_registry.activate(dispatch)` 把 attempt 注册到 `DispatchAttemptRegistry`，供 `/v1/dispatch/stop` 端点反查；
6. 拆 `MOTOR_PREFILL_RESULT_KEY`（prefill 的 KV bootstrap），调用 `_consume_prefill_result` 注入到 `engine_body`；
7. `_adapt_engine_body` 是子类钩子，把 envelope 转成引擎侧字段；
8. 任何异常触发 `stop_peer` 通知对端取消、`_registry.finish` 把 attempt 标记 done。

`DispatchAttemptRegistry`（`base.py:126-289`）是 EngineServer 端的 attempt 状态机：

- `activate(dispatch)`：状态 `active`，写入 `(root_request_id, attempt_seq) → MotorDispatch`；
- `cache_prefill_body(dispatch, body)`：缓存 prefill 请求 body 给 metaserver 回调用，唤醒 `wait_prefill_entry` 的 waiter；
- `stop(stop_request)`：接收对端的取消，按 `(pair_id, attempt_seq)` 校验防 stale，返回 `STOPPED/ALREADY_STOPPED/ALREADY_DONE/NOT_FOUND/STALE`；
- `_cleanup_locked`：TTL（默认 300s）过后清理 `(done, stopped)` 状态的 attempt 与 prefill body 缓存，避免内存膨胀。

## VLLMDispatchAdapter

`VLLMDispatchAdapter`（`motor/engine_server/core/dispatch_adapter/vllm_adapter.py:39`）实现两套 vLLM dispatch profile：

- **metaserver**（默认）：`do_remote_prefill=True`，prefill 端把 KV 推到 decode 端的 `/v1/metaserver`；
- **handoff**（CPCD / Mooncake）：`do_remote_decode=True`，prefill 端把 `kv_transfer_params`（含 `remote_block_ids`/`remote_host`/`remote_port`/`remote_engine_id`）作为 `PrefillResult` 回给 coordinator，coordinator 再转给 decode 端。

`_adapt_engine_body`（`vllm_adapter.py:58-86`）：

- 写入 `request_id = dispatch.engine_request_id`；
- decode role 且使用 metaserver profile：把 `kv_transfer_params` 注入为 `metaserver` URL；
- prefill role：默认 `return_token_ids=True`，并把 `max_tokens=1`/`stream=False` 收紧参数；
- prefill role + handoff：注入 `do_remote_decode=True` 触发 prefill 返回 KV bootstrap。

`maybe_prepare_response`（`vllm_adapter.py:88-103`）在 prefill role 使用 handoff 时返回 `PrefillResult(status="prepared")`，让 EngineServer 框架在收到真实 prefill body 时把它缓存到 `DispatchAttemptRegistry`，等 metaserver 回调来取。

`_consume_prefill_result`（`vllm_adapter.py:118-138`）是反向：在 decode role 且 handoff 时，把 `PrefillResult.payload` 拷贝到 `engine_body["kv_transfer_params"]`，让 vLLM 的 connector 用这些 bootstrap 字段直接拉 KV。

`prepare_metaserver_request`（`vllm_adapter.py:144-172`）实现 decode 端"接收 metaserver 回调"的入口：抽取 `kv_transfer_params` → `engine_request_id`，从 `_registry.wait_prefill_entry` 拉 prefill 缓存（30s 超时）→ 校验 KV 字段必填（`_METASERVER_REQUIRED_FIELDS`，`vllm_adapter.py:43-52`）→ 返回 dispatch + 调整后的 body。

`normalize_response`（`vllm_adapter.py:174-225`）：

- handoff prefill：把响应 body 中的 `kv_transfer_params` 包成 `PrefillResult(status="completed", handoff_mode="handoff", payload=kv_transfer_params, usage=...)`；
- 其他情况：调用 `normalize_nonstream_body`（来自 `normalization.py`）按 `client_expects_chat_shape` 把 `text_completion` 转 `chat.completion`，按 `client_return_token_ids` 裁掉 token id。

`normalize_stream_chunk`（`vllm_adapter.py:227-241`）按 SSE 块处理：`parse_stream_chunk_json` → 修改 → `encode_stream_chunk_bytes`，确保 SSE 边界不被破坏。

## SGLangDispatchAdapter

`SGLangDispatchAdapter`（`motor/engine_server/core/dispatch_adapter/sglang_adapter.py:22`）的契约简单很多——SGLang 的 PD 协议是 bootstrap 模式，仅需在 body 里塞入 bootstrap 三元组：

```python
async def _adapt_engine_body(self, body, dispatch):
    body["request_id"] = dispatch.engine_request_id
    prefill = dispatch.endpoints.prefill
    if prefill is None:
        return body
    parsed = urlparse(prefill.url)
    bootstrap_port = os.getenv("DISAGGREGATION_BOOTSTRAP_PORT", "").strip()
    if not bootstrap_port:
        raise HTTPException(status_code=500, detail="DISAGGREGATION_BOOTSTRAP_PORT must be set ...")
    body.update({
        "bootstrap_host": parsed.hostname or prefill.url,
        "bootstrap_port": bootstrap_port,
        "bootstrap_room": self._stable_bootstrap_room(dispatch),
    })
    return body
```

`_stable_bootstrap_room`（`sglang_adapter.py:46-49`）用 BLAKE2b 把 `(pair_id, attempt_seq)` 散成 63 位正整数，保证同一 attempt 的 P/D 都看到同一个 bootstrap_room，SGLang 能正确配对 KV 通道。

SGLang 没有 prefill result 概念——dispatch 协议完全靠 bootstrap 字段握手，所以 `_consume_prefill_result`/`maybe_prepare_response` 走基类的 no-op 默认。

## 公共能力

`DispatchAdapter` 基类还提供：

- `handle_stop(body)`（`base.py:384-404`）：处理对端的 `DispatchStopRequest`，返回 `accepted`/`state` 字段；
- `finish_dispatch(dispatch)`（`base.py:406-408`）：把 attempt 标记 `done`，清理 prefill 缓存；
- `is_dispatch_stopped(dispatch)`（`base.py:410-413`）：`_registry.is_stopped` 短路查询；
- `stop_peer(dispatch, reason)`（`base.py:415-422`）：调 `DispatchPeerStopClient.stop_peer`（`base.py:62-123`）发 `/v1/dispatch/stop` 给对端，1s 超时；
- `map_engine_error`（`base.py:370-382`）：兜底把引擎异常翻译成 `{"error": {"type", "message", "code"}}`。

## EngineServer 与 DispatchProfile 的协作

`VLLMDispatchAdapter._infer_dispatch_profile`（`vllm_adapter.py:290-296`）：

```python
@classmethod
def _infer_dispatch_profile(cls, config) -> DispatchProfile:
    endpoint_config = config.get_endpoint_config()
    deploy_config = getattr(endpoint_config, "deploy_config", None)
    engine_config = getattr(deploy_config, "engine_config", None)
    explicit_profile = getattr(deploy_config, "dispatch_profile", None)
    return classify_vllm_dispatch_profile(engine_config, explicit_profile=explicit_profile)
```

`classify_vllm_dispatch_profile` 根据 `engine_config` 中的 `kv_connector` 名称推断（HANDOFF/DEFAULT），或尊重用户在 `deploy_config.dispatch_profile` 显式指定的值。结果决定 `_adapt_engine_body` 走 handoff 还是 metaserver 路径。

`DispatchEndpoint`（`dispatch_session.py:209-217`）把 `ScheduledResource` 转成 dispatch envelope 里的 `instance_id`/`endpoint_id`/`url` 三元组，确保 coordinator→engine 的路径完全可重建。

## 跨文档引用

- `UnifiedPDRouter` 如何构造 `MotorDispatch` 见 `T2_02_1_统一PD路由逻辑.md`。
- `PDDispatchSession.dispatch_for` 把 `AttemptContext` 转 envelope 见 `T2_06_StreamResponse.md`。
- Coordinator 端 stop 流程与 `_stop_attempt` 见 `T2_02_1_统一PD路由逻辑.md`。
- `DispatchStopClient`（Coordinator 侧的对端停止客户端）与 EngineServer 端 `/v1/dispatch/stop` 端点配对。
