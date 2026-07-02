# T8_03 ZMQ 协议设计

Worker 与 Scheduler 之间除了高频的 workload 共享内存读写，还需要低频但严格一致的请求-响应式交互：「给我一个 ALLOCATION」「更新某个 endpoint 的 workload」「确认精度采样」等。PyMotor 用 ZMQ ROUTER/DEALER 模式承载这一交互。本节给出协议的消息类型、序列化方式、会话管理与错误处理。所有引用集中在 `motor/coordinator/scheduler/runtime/zmq_protocol.py`。

## 整体拓扑：ROUTER/DEALER 多进程扩展

`zmq_protocol.py:L9-L15` 的模块注释明确说明使用 ROUTER/DEALER 模式：

- **Scheduler 进程**持有 ROUTER socket：作为服务端，能同时接受多个 Worker 的连接并把响应路由回正确的 Worker。
- **每个 Worker**持有 DEALER socket：作为客户端，与 ROUTER 通信。
- ROUTER/DEALER 是异步的，天然支持多 Worker 横向扩展——这是 PyMotor 能在 `inference_workers_config.num_workers > 1` 时维持高吞吐的关键。

地址与超时配置在 `CoordinatorConfig.scheduler_process_config`（`motor/config/coordinator.py:L248-L266`）：

- `frontend_address = "ipc://<ipc_dir>/scheduler_frontend"`：ROUTER 端点。
- `instance_pub_address = "ipc://<ipc_dir>/scheduler_instance_pub"`：实例变更 PUB 端点（Worker 订阅）。
- `timeout = 5.0`：客户端请求超时。
- `reconnect_interval = 5.0`：客户端重连间隔。

## 消息类型：SchedulerRequestType 与 SchedulerResponseType

`zmq_protocol.py:L28-L49`：

```
class SchedulerRequestType(str, Enum):
    ALLOCATE_ONLY            = "allocate_only"          # Worker 选实例，Scheduler 只分配 workload
    UPDATE_WORKLOAD          = "update_workload"
    GET_AVAILABLE_INSTANCES  = "get_available_instances" # Worker 取实例列表与 SHM 名
    REFRESH_INSTANCES        = "refresh_instances"
    CONFIRM_SAMPLE           = "confirm_sample"          # 跨 Worker 精度采样退出门
    RECORD_PRECISION_RESULT  = "record_precision_result" # 全局连续计数 + 探测
    FINISH_PRECISION_ACTION  = "finish_precision_action" # 探测后清理

class SchedulerResponseType(str, Enum):
    SUCCESS = "success"
    ERROR   = "error"
```

注意一个细节：协议里**没有** `IS_AVAILABLE` 之类的「问某个实例在不在」请求。注释（同文件 L29-L31）解释了原因——「Scheduler 进程使用本地 InstanceManager 做只读查询」，不需要 worker 远程发起状态查询。这减少了 ROUTER 上的请求量。

`ALLOCATE_ONLY` 这一类型的命名也体现了设计哲学：Worker 在本进程内已经用本地视图（结合 SHM workload 数据）选好了候选 instance/endpoint，Scheduler 只负责权威地确认并扣减 workload ledger。这与 `kv_cache_affinity.py` 中 worker 先做候选排序再让 scheduler 二次仲裁的设计一致——见 `kv_cache_affinity.py:L62-L72` 的注释。

## 消息结构：SchedulerRequest / SchedulerResponse

`zmq_protocol.py:L52-L70`：

```
class SchedulerRequest(msgspec.Struct):
    request_type: str              # SchedulerRequestType 值
    request_id:   str              # 对应 RequestInfo.req_id
    data:         dict[str, Any]   # 业务数据

class SchedulerResponse(msgspec.Struct):
    response_type: str             # SUCCESS / ERROR
    request_id:   str
    data:         dict[str, Any] | None = None
    error:        str | None       = None
```

`msgspec.Struct` + msgpack 是协议选型的关键：

- 比 pickle 安全（明确的 schema，无任意对象执行风险）。
- 比 JSON 快（msgpack 二进制 + 强类型 Struct）。
- `request_id` 在 Request 与 Response 之间承担**会话关联**职责——DEALER 是异步的，多个请求 in-flight 时需要靠 `request_id` 匹配响应。

## 序列化与零拷贝：ZMQMessageSerializer

`zmq_protocol.py:L156-L222` 的 `ZMQMessageSerializer`：

- 用 `msgspec.msgpack.Encoder(enc_hook=_enc_hook)` 编码，`enc_hook` 把 `Enum` 转为 `value`、把 `Pydantic BaseModel` 直接 `.model_dump(mode='json')`，避免重复序列化。
- 零拷贝策略（`zmq_protocol.py:L85-L87`）：
  - `DEFAULT_ZERO_COPY_THRESHOLD = 1024`：序列化后字节数超过这个阈值的 dict/list 字段，会被单独切到一个 ZMQ 帧（aux frame），主帧只保留 `{"__ref__": index}`。
  - `_replace_large_with_refs` 在序列化前递归扫描，把大对象「外提」成 aux frame。
  - `_resolve_refs` 在反序列化时递归把 `__ref__` 还原回原对象。
- 这套设计的关键收益：**降低 ZMQ 单帧大小、减少反序列化的内存峰值**。`ALLOCATE_ONLY` 请求的 data 通常很大（候选 instance 列表、workload 字典），如果不切 frame 会一次性 allocate 一个巨大的 Python 对象。

会话建立后，发送时用 `pack_send_frames(prefix, payload)`（同文件 L225-L229），`prefix` 是 ROUTER/DEALER 必需的 routing prefix（identity frame 或空 frame），`payload` 是单帧或多帧。接收时 `unpack_recv_payload(parts, payload_start=1)`（同文件 L232-L238）跳过 routing prefix，提取业务 frame。

## 会话管理：request_id 的角色

DEALER 是异步的——Worker 可以在同一连接上发出 N 个请求而不等待响应，每个响应都带 ROUTER routing prefix + 业务 frame。Worker 侧的会话管理靠 `request_id`：

1. Worker 构造 `SchedulerRequest(request_type=..., request_id=req.req_id, data=...)` 发送。
2. Scheduler 处理后回 `SchedulerResponse(request_id=req.req_id, ...)`。
3. Worker 通过 `request_id` 把响应 match 回具体的 `RequestInfo`，完成后续的 workload 释放或状态推进。

`request_id` 同时承担了「业务追踪」与「协议去重」两个职责——同一个 `RequestInfo.req_id` 在协议层只对应一个 in-flight 请求，避免重复 ALLOCATE/UPDATE。

## 候选策略枚举

`zmq_protocol.py:L73-L83`：

```
CANDIDATE_POLICY_LOAD_BALANCE      = "load_balance"
CANDIDATE_POLICY_ROUND_ROBIN       = "round_robin"
CANDIDATE_POLICY_KV_CACHE_AFFINITY = "kv_cache_affinity"
KNOWN_CANDIDATE_POLICIES = frozenset({...})
```

Worker 在 ALLOCATE_ONLY 请求的 `data["candidate_policy"]` 里指明自己用的策略；Scheduler 据此选择对应的 Policy 实例（`LoadBalancePolicy` / `RoundRobinPolicy` / `KvCacheAffinityPolicy`）。这套枚举与 `CoordinatorConfig.scheduler_config.scheduler_type`（`motor/config/coordinator.py:L102-L114`）保持一致。

## 错误处理：response_type = ERROR

`SchedulerResponse.response_type = "error"` 时，`error` 字段携带错误字符串，data 通常为 None。Worker 收到后按业务语义处理——例如 ALLOCATE_ONLY 失败会触发上层重试逻辑（见 `motor/coordinator/router/strategies/base.py:L341-L364`）。

协议层不强制区分错误类型（HTTP-style status code），原因是 ZMQ 内部错误传播不需要跨进程状态——`error` 字段的字符串对调用方足够定位问题。

## 与共享内存的协作

ZMQ 与 SHM 不是替代关系而是协作关系：

- **SHM**：承载 workload 高频读，纳秒级；Worker 每次请求先读 SHM 决定候选。
- **ZMQ**：承载 ALLOCATE / UPDATE / REFRESH 等需要权威一致性或低频但严格一致的事件，毫秒级。
- **PUB/SUB**：`INSTANCE_CHANGE_TOPIC = b"instances_changed"`（`zmq_protocol.py:L90`），Scheduler 在 `REFRESH_INSTANCES` 时向 PUB 推送 multipart `[topic, version_bytes]`，所有 Worker 收到后触发本地 instance 缓存刷新。

这套组合让 Worker 既能拿到高频 workload 数据，又能保证实例列表变更不延迟——后者通常对应 Pod 重启、role 重分配等集群级事件，是 SHM 的 `instance_version` 字段所不能完全覆盖的。

## 协议演进

`SCHEMA_VERSION = 1`（`layout.py:L28`）是 SHM 的版本号；ZMQ 协议没有显式版本号，但有以下两个演进机制：

- **`data` 字段的 schema 是隐式的**：新增字段时旧 Worker 不识别会忽略，破坏兼容性靠测试保证。
- **`msgspec.Struct` 的字段顺序**：新增字段必须放在末尾，避免破坏序列化字节序。

生产部署时建议遵循「先升 Worker 后升 Scheduler」的灰度顺序——Worker 在版本不一致时通常只会拿到错误响应或丢失部分字段，不会导致进程崩溃。