# T8_04 端到端 Tracing 链路

MindIE PyMotor 的请求从 Coordinator 入口到 EngineServer 出口要经过多次「角色」切换（API Server → Router → Scheduler → EngineServer），每一步都可能引入延迟或异常。Tracing 的目的是把这一次完整调用链串起来：让运维方能在 trace 后端看到「Prefill 耗时 X ms / Decode 耗时 Y ms / TTFT Z ms」这样的可观测信号。本节讲 TraceID 怎么在请求生命周期中传播、TracerManager 怎么管理 trace context、Span 与 TraceObj 怎么对应。代码集中在 `motor/coordinator/tracer/tracing.py`。

## 协议基础：W3C TraceContext

`tracing.py:L45-L46`：

```
_TRACE_HEADERS = ("traceparent", "tracestate")
_TEXTMAPPROPOGATOR = TraceContextTextMapPropagator()
```

PyMotor 选 W3C TraceContext 作为传播协议——这是 OpenTelemetry 的默认传播器，traceparent / tracestate 两个 header 在跨服务调用时被注入与提取。`TracerManager.extract_trace_context(headers)`（L157-L162）和 `inject_trace_context()`（L164-L169）是这套协议的两个入口：

- `extract`：从 HTTP 头里读 `traceparent` / `tracestate`，构造 OpenTelemetry `Context`。后续 Span 创建时会用这个 Context 作为 parent，让本次请求的 Span 与上游服务串成一条调用链。
- `inject`：把当前活跃 Span 的 `traceparent` / `tracestate` 写入新的 dict，转发给下游服务。

`contains_trace_headers(headers)`（L171-L173）判断头里是否携带 trace header——只有携带时才需要把上下文接到上游。

## TracerManager：单例 + 双层采样

`tracing.py:L39-L155` 的 `TracerManager` 是 `ThreadSafeSingleton`（同文件 L39 + L32 的导入），全进程共用一个实例。`_initialized`（L50）保证只 init 一次。

### 采样策略

`tracing.py:L106-L127` 的 `init_tracer(enabled=True)`：

```
sampler = ParentBased(
    root=TraceIdRatioBased(self.root_sampling_rate),
    remote_parent_sampled=TraceIdRatioBased(self.remote_parent_sampled),
    remote_parent_not_sampled=TraceIdRatioBased(self.remote_parent_not_sampled),
    local_parent_sampled=TraceBased(self.local_parent_sampled),
    local_parent_not_sampled=TraceBased(self.local_parent_not_sampled),
)
```

这是 OpenTelemetry `ParentBased` + 5 个 `TraceIdRatioBased` 的组合：

- `root`：上游无 parent 时（即客户端没带 trace header）的采样率。
- `remote_parent_sampled` / `remote_parent_not_sampled`：上游带 trace header 且标记 sampled / not-sampled 时的二次采样概率——主要用于上游采了但本服务希望降采样的场景。
- `local_parent_sampled` / `local_parent_not_sampled`：本服务内部 Span 之间的采样策略。

`get_protocol()`（L144-L155）根据 `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` 环境变量决定导出协议是 `grpc` 还是 `http/protobuf`；若 `tracer_config.endpoint` 为空，tracing 直接禁用（`NoOpTracerProvider`），不会有任何开销。

### 配置更新

`update_config(config)`（L72-L90）：支持配置热更新，先 shutdown 旧 provider 再 init 新 provider。配置 reload 场景下需要这个能力——避免老的 BatchSpanProcessor 还在向已废弃的 endpoint 写数据。

## TraceObj：请求级追踪状态

`tracing.py:L176-L313` 的 `TraceObj`（`@dataclass(config=ConfigDict(arbitrary_types_allowed=True))`）挂在 `RequestInfo.trace_obj` 上，跟随请求走完整个生命周期。它的字段：

- `time_start`：请求开始时间（`set_time_start` 调用时记录）。
- `time_first_token`：首个 token 到达时间（`set_time_first_token` 调用时记录）。
- `count_token`：生成的 token 数。
- `error_message` / `meta_error_message`：业务错误信息，分主 span / meta span 两份。
- `parent_context`：上游 trace context（`extract_trace_context` 的结果）。
- `span`：主 Span，`UnifiedPD` / `PDHybrid` / `CDP_Encode` 等主要流程的 span 入口。
- `meta_span`：meta span，多模态 Encode 阶段或 metaserver 转发阶段的 span。
- `trace_headers` / `meta_trace_headers`：当前 span 的传播头，用于转发给 EngineServer。

注意 meta 与非 meta 的二分：`is_meta=True` 时所有 set/add/inject 操作都打在 meta_span/meta_trace_headers 上。这是为了支持「PD 分离请求 + 编码前置」这种多层流水线——主 span 记录整体 TTFT/TPOT，meta span 记录 Encode 阶段耗时。

## 端到端链路：从入口到出口

下面以一条「OpenAI chat 走 P/D 分离」请求为例，看 trace 怎么串：

### 1. Coordinator 入口：trace 头提取

请求进入 Coordinator 的 OpenAI 兼容端点，`BaseRouter._trace_span(span_name, is_stream)`（`motor/coordinator/router/strategies/base.py:L192-L204`）被调用：

```
with TracerManager().tracer.start_as_current_span(span_name, context=trace_obj.parent_context) as span:
    trace_obj.span = span
    trace_obj.trace_headers = TracerManager().inject_trace_context()
    trace_obj.set_trace_attribute("requestId", self.req_info.req_id)
    trace_obj.set_trace_attribute("stream", is_stream)
```

这里：

- `trace_obj.parent_context` 是上一阶段（API Server 入口）`extract_trace_context` 的结果。
- `inject_trace_context()` 在 Span 创建后立即注入新 trace_headers，准备发给 EngineServer。
- `set_trace_attribute("requestId", ...)` 把 PyMotor 自己的请求 ID 写到 Span 上，便于跨服务关联。

### 2. Router 分发：trace 携带跨进程

Router 通过 `_manage_resource_context` / `_create_attempt` 调用 `_scheduler.select_and_allocate`，这一步对 EngineServer 不可见——只是 Coordinator 内部状态机推进。但 `_manage_resource_context` 会在 trace 上加事件：

```
trace_obj.add_trace_event("Begin Scheduled Resource", is_meta=...)
trace_obj.add_trace_event("Scheduled Resource ok", attributes={...}, is_meta=...)
```

把调度耗时与 instance/endpoint 信息作为 Span Event 记录——方便在 trace 后端看到「调度阶段花了 X ms」。

### 3. 转发 EngineServer：trace 头注入

`BaseRouter.forward_request` / `forward_stream_request`（同文件 L397-L548）：

```
headers = {'Content-Type': 'application/json', 'X-Request-Id': self._forward_request_id(req_data)}
headers.update(trace_obj.get_trace_headers_dict(self.is_meta))
```

`get_trace_headers_dict(is_meta)`（`tracing.py:L274-L282`）返回当前 Span 的传播头，被合并到 HTTP 请求头里发给 EngineServer。这样 EngineServer 收到请求时 trace header 已经是当前 Span 的 child 关系——后续在 EngineServer 内部做的 span 会自动挂到 PyMotor 的 Span 下。

### 4. EngineServer 处理：trace 透传

EngineServer 不需要懂 PyMotor 的 TraceObj，但应该把 `traceparent` / `tracestate` 透传给推理引擎（vLLM / SGLang），让引擎内的 span 也串到同一条 trace 上。OpenTelemetry 的 SDK 会自动处理 child context 提取——EngineServer 只需要在创建 span 时把 trace headers 作为 parent 传入即可。

### 5. 响应回包：TTFT / TPOT 记录

`BaseRouter.forward_stream_request`（同文件 L420-L457）在首个 chunk 到达时：

```
self.first_chunk_sent = True
trace_obj.set_time_first_token()
self.req_info.update_state(ReqState.FIRST_TOKEN_FINISH)
```

`TraceObj.set_end_and_ttft_tpot`（`tracing.py:L218-L238`）在请求结束时计算并写回：

- `TTFT = (time_first_token - time_start) / 1ms`
- `TPOT = (time_end - time_first_token) / (count_token * 1ms)`

写到 Span 的 attribute 上，trace 后端可以直接看到这两个核心延迟指标。

## Meta span：多模态 Encode 与 metaserver 场景

`BaseRouter.do_encode`（同文件 L612-L669）专门为多模态 Encode 创建 meta_span：

```
with TracerManager().tracer.start_as_current_span("CDP_Encode", context=trace_context) as span:
    self.is_meta = True
    trace_obj.meta_span = span
    trace_obj.meta_trace_headers = TracerManager().inject_trace_context()
    trace_obj.set_trace_attribute("requestId", self.req_info.req_id, is_meta=True)
```

这是「CDP 模式」（Encode + Prefill + Decode 三段）的 span 入口，Encode 阶段的所有 set/add/inject 都用 `is_meta=True`，与主 span 隔离但共享 parent_context。`UnifiedPDRouter.handle_metaserver_request`（`motor/coordinator/router/strategies/unified_pd.py:L907-L935`）也走 meta span——用于 D 实例端的「P 端预处理」回包请求。

## 错误与异常在 Trace 上的表达

`tracing.py:L240-L313` 提供四个方法：

- `set_trace_error_message(error_log, is_meta)`：把业务错误字符串写到 Span 的 attribute + event。
- `set_trace_exception(exception, is_meta)`：调用 OpenTelemetry `span.record_exception(exception)`，记录堆栈。
- `set_trace_status(exception, is_meta)`：把 Span 状态设为 ERROR，并把异常名+消息写到 status description。
- `set_trace_attribute(key, value, is_meta)`：写任意 attribute。

`BaseRouter._process_response_error`（`unified_pd.py:L245-L292`）在重试/终止分支调用这三个方法，保证 trace 后端能看到完整的失败原因——这对线上 debug 至关重要。

## 关闭与 Flush

`TracerManager.shutdown()`（`tracing.py:L92-L104`）：进程退出前调用，确保 `BatchSpanProcessor` 的待发送 span 被 flush 出去。这是 OTel SDK 的标准要求——否则进程退出时丢 span 是常见问题。

PyMotor 在 Coordinator 进程退出路径里会调用此 shutdown，配合 OTLP exporter 的 HTTP/gRPC 发送，保证 trace 数据完整落地。

## 跨服务调试的实用建议

- 看 `TTFT(ms)` attribute：如果数值大，多半是调度耗时或 Prefill 耗时；前者查 `add_trace_event("Scheduled Resource ok")` 前的 event，后者查 EngineServer 的 Prefill span。
- 看 `TOKEN_COUNT`：流式响应里非空 chunk 数（`set_count_token` 在 L477-L478 设置），用于验 TPOT 计算分母。
- 看 `error.message`：业务语义错误；与 `record_exception` 的 stacktrace 配合定位是逻辑错误还是引擎错误。

Trace 链路的完整设计与 OpenTelemetry SDK 的协作成为了 PyMotor 在生产环境中可观测性的基础；与 Logs（`motor/common/logger`）和 Metrics（`prometheus_metrics_config`）一起构成完整的三件套。