# T8_02 SharedMemory 布局

Scheduler 与 Worker（Inference worker）之间需要共享「每个 endpoint 的 workload」才能保证调度决策的全局一致性——如果 Worker 只看本地 workload，会出现多 Worker 同时把请求路由到同一热点 endpoint 的问题。PyMotor 通过共享内存（SHM）让 Scheduler 把权威 workload 推给所有 Worker，Worker 读 SHM 完成本地视图同步。本节给出这套 SHM 的布局、写入协议与读取协议。相关代码集中在 `motor/coordinator/scheduler/runtime/workload_shm/`。

## 为什么用 SHM 而不是 RPC

Worker 每收到一条请求都要查一次 workload 以决定路由。如果用 RPC，每次都要走 ZMQ 序列化/反序列化 + 跨进程内存拷贝，会成为高 QPS 场景下的明显瓶颈。SHM 的好处：

- **零拷贝**：Worker 通过 `multiprocessing.shared_memory.SharedMemory` 拿到原始字节流，`memoryview` 直接 cast 为 `WorkloadShmEntry`，没有 Python 对象层的中转。
- **高频读低频写**：Worker 读 SHM 是纳秒级；Scheduler 写 SHM 的频率由 workload 变更触发（每次 `update_workload` 约 1-5µs，见 `writer.py:L156-L177` `write_single_entry` 的注释）。
- **无锁读**：通过 seqlock 协议让 Worker 读到一致快照即可，不需要显式加锁。

## 总体布局：64B 头部 + N × 32B 条目

`motor/coordinator/scheduler/runtime/workload_shm/layout.py:L40-L48`：

```
HEADER_SIZE = 64
HEADER_FMT  = "<I H H q I I Q Q 24x"  # magic, schema, padding, sequence, entry_count, max_entries,
                                       # instance_version, heartbeat_sequence, reserved(24B)
ENTRY_SIZE  = 32
ENTRY_FMT   = "<i i B 3x d d 4x"        # instance_id, endpoint_id, role(1B+pad),
                                       # active_tokens(double), active_kv_cache(double)
```

整个 SHM 是「Header + Entries[]」的两段式结构。`DEFAULT_WORKLOAD_SHM_MAX_ENTRIES = 10240`（同文件 L51），对应理论上限 64B + 10240×32B = 327744B ≈ 320KB，足以覆盖数千个 endpoint。

## Header 字段语义

Header 是 Scheduler 与 Worker 之间「状态一致性」的根。每个字段都承载一个具体的协议角色：

- **magic（4B）**：ASCII `"WKLD"`（`0x574B4C44`，同文件 L25）。Worker 在 attach SHM 后先读 magic，校验失败立刻返回——避免把别的进程的共享内存误读成 workload SHM。
- **schema_version（2B）**：当前固定为 1（同文件 L28）。未来 Header 字段或 Entry 字段变化时递增，Worker 据此判断布局兼容性。
- **sequence（8B）**：seqlock 序列号。**奇数表示 Scheduler 正在写入，偶数表示稳定快照**。Worker 用这个字段实现无锁读：先读到偶数 → 解码所有条目 → 再读一次 Header，若 sequence 仍为同一偶数且其他字段一致，则读到的快照有效。
- **entry_count（4B）**：当前有效的 Entry 数量。Worker 据此决定解多少个 Entry。
- **max_entries（4B）**：Entry 区容量上限。Worker 据此校验 SHM 实际大小是否足够。
- **instance_version（8B）**：实例列表版本号，每次 `write_snapshot` 递增。Worker 用这个字段判断本地 instance 缓存是否需要全量刷新——见 `writer.py:L114-L116` 的注释。
- **heartbeat_sequence（8B）**：心跳序号，Scheduler 大约每秒写一次（同文件 L43 的 `HEARTBEAT_STALE_SEC = 5.0` 是 stale 阈值）。Worker 用这个字段判断 Scheduler 是否还活着——如果 heartbeat_sequence 在 5 秒内没变化，Worker 把 SHM 视为 stale 并触发 `get_available_instances` 重新同步。

这四个 8B 字段（sequence / instance_version / heartbeat_sequence + reserved）共同支撑了 seqlock + 版本控制 + 心跳检测三套并行的协议。

## Entry 字段语义

`layout.py:L54-L61` 的 `WorkloadShmEntry`：

```
instance_id:    int     # 4B
endpoint_id:    int     # 4B
role:           int     # 1B (0=P, 1=D, 2=hybrid, 3=E)
padding:        3B
active_tokens:  float64 # 8B
active_kv_cache:float64 # 8B
padding:        4B
```

每条 Entry 描述一个 (instance, endpoint) 对的 workload。`role` 字段映射见 `writer.py:L41-L49` 的 `_pdrole_to_shm_role` 与 `reader.py:L39-L47` 的 `_shm_role_to_pdrole`——双向映射确保 Scheduler 写入和 Worker 读取对角色语义的理解一致。

`active_tokens` 与 `active_kv_cache` 是 float64 而非整数，因为 KV 亲和策略中需要做 `prefill_load_scale * prefill_cost + load_weight * load_cost` 这类浮点打分（`kv_cache_affinity.py:L326-L329`），用 float64 保证打分精度不会被截断。

## Writer：Scheduler 侧写入协议

`motor/coordinator/scheduler/runtime/workload_shm/writer.py`：

- `write_snapshot()`（L135-L154）：全量重建。当实例列表发生变化（Pod 新增/删除/role 变化）时调用，重新计算 `_slot_map` 并把所有 Entry 写入对应 slot。**会递增 `instance_version`**。
- `write_single_entry(instance_id, endpoint_id)`（L156-L177）：增量写入。每次 workload 变化（约 1-5µs）只更新对应 slot 的 32B Entry，不需要重建 `_slot_map`。
- `write_heartbeat()`（L128-L133）：周期性心跳写入，单独写 `heartbeat_sequence` 字段。

写入时序通过 `_begin_write` / `_end_write` 维护：

```
_begin_write: sequence += 1 (奇数)   # 标记写中
写 entry ...
_end_write:   sequence += 1 (偶数)   # 标记完成
```

`pack_header` 写入时还会先读 SHM 当前 heartbeat 值（`_write_header` L195-L204），取 max 后再写回——保证不会因 workload 写入而覆盖更新的 heartbeat。

## Reader：Worker 侧读取协议

`motor/coordinator/scheduler/runtime/workload_shm/reader.py`：

- `attach()`（L68-L71）：用 `shared_memory.SharedMemory(name=..., create=False)` 附加到 Scheduler 创建的 SHM 段。
- `read_and_patch_cache(cache)`（L84-L123）：核心读取入口。流程：
  1. 循环 `STABLE_SNAPSHOT_READ_ATTEMPTS = 3` 次（避免极小概率的 seqlock 冲突）。
  2. 读 Header，若 `sequence % 2 == 1` 表示 Scheduler 正在写，重试。
  3. 解码 `entry_count` 个 Entry。
  4. **再次** 读 Header，验证 `sequence` / `magic` / `entry_count` / `instance_version` 与第一次一致；若都一致则视为有效快照。
  5. 通过 `_update_heartbeat_and_check_stale` 跟踪心跳变化；若 heartbeat 在 `HEARTBEAT_STALE_SEC = 5.0` 内未变则标记 stale。
  6. 把 Entry 写入 `cache.patch_workload_from_shm(...)` 刷新本地缓存。

`detach()`（L73-L82）释放 memoryview 后再 close SHM——这一步顺序很关键，因为如果在 close SHM 前不释放 memoryview 会触发 `BufferError: cannot close exported pointers exist`。

## 跨进程场景下的关键约束

整套 SHM 协议依赖两个跨进程不变式：

1. **字节序一致**：所有 struct 都是 little-endian，跨进程跨架构时也要保持。
2. **schema_version 一致**：Worker 与 Scheduler 升级时若 layout 变化，必须先升 Worker 再升 Scheduler 或反之，避免 Worker 解错 Entry 字段。

第三条工程上不那么显眼但同样重要：**SHM 容量规划**。`DEFAULT_WORKLOAD_SHM_MAX_ENTRIES = 10240` 是硬编码上限，不是用户可配的——当集群规模超过这个数时 `write_snapshot` 会截断（`writer.py:L66-L70` 的 `len(entries) >= max_entries` 检查），并打印 warning。生产部署前需要评估集群规模，确保 `(instance × endpoint)` 总数不超限。

## 与 ZMQ 协议的分工

SHM 只解决「高频 workload 同步」；低频但必须严格一致的事件（如「实例列表整体变化」「心跳检测」）通过 ZMQ 推送，二者职责清晰：

- **SHM**：每条请求都要读，承载 workload 数据，seqlock 保证一致性。
- **ZMQ**：Scheduler 主动推送 `REFRESH_INSTANCES` 或心跳，告诉 Worker「SHM 可能已经过期了，触发一次全量同步」。

两套机制协同工作：Worker 每条请求先读 SHM（纳秒级），周期性或事件触发再走 ZMQ 拿权威实例列表（毫秒级）。详见 `T8_03_ZMQ协议设计`。