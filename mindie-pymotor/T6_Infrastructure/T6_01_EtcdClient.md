# T6_01 EtcdClient：分布式状态存储、租约锁与持久化

MindIE Motor 选择 etcd 作为分布式一致性存储，承担三类职责：跨 Controller/Coordinator 的共享状态持久化、Master/Standby 主备选举的租约锁、以及引擎实例元信息的发布/订阅。本节梳理这三类交互在 `motor/common/etcd/` 下的代码实现。

## 一、EtcdClient：gRPC 通道与键命名空间

`motor/common/etcd/etcd_client.py:43` 的 `EtcdClient` 类基于 etcd v3 gRPC 协议封装，使用 `rpc_pb2_grpc.KVStub` 与 `LeaseStub` 两个桩对象完成 K/V 操作与租约管理。构造函数 `etcd_client.py:46-89` 完成三件关键准备：

1. **gRPC 通道配置**：通过 `grpc.service_config` 设置负载均衡策略 `etcd_lb_policy`，支持 etcd 集群多节点访问。
2. **TLS/非 TLS 自适应**：当 `tls_config.enable_tls` 为真时，读取 CA、cert、key 三份 PEM 并通过 `grpc.ssl_channel_credentials` 构建 `secure_channel`；否则走 `insecure_channel`。这条分支决定了集群间通信是否加密。
3. **桩对象持有**：`kv_stub` 与 `lease_stub` 在长生命周期中复用，避免每次读写都重建连接。

### 键命名空间隔离

多租户场景下，Controller 与 Coordinator 共用一个 etcd 集群时必须避免键冲突。`get_key_with_namespace_and_job_name` (`etcd_client.py:97-102`) 用环境变量 `POD_NAMESPACE` 与 `JOB_NAME` 自动拼接前缀，所有写入路径都先经此函数处理：

```python
return namespace + "/" + job_name + key
```

这样不同 namespace/job 的数据天然隔离；同时对已经是全限定路径的键做幂等保护，避免重复拼接。

## 二、Lock：基于 etcd 事务的租约锁

`motor/common/etcd/locks.py:23-101` 实现的 `Lock` 是 Motor 分布式锁的最小语义单元。每个锁由三个 etcd 资源构成：

| 资源 | 用途 |
|------|------|
| Lease（TTL=60s 默认） | 锁存活期的最后防线 |
| 键 `/locks/<name>` | 锁的实际持有标记，值为 UUID 标识持有者 |
| Compare+Txn | 创建时通过 `Compare.CREATE` + `create_revision=0` 判定"键不存在"才能写入 |

`acquire` 的事务结构 (`locks.py:75-79`) 是 etcd 分布式锁的经典模式：

- **Compare**：`create_revision == 0`，仅当键从未被创建过才进入 success 分支；
- **Success**：写入 `<key, uuid, lease=lease_id>`；
- **Failure**：回读当前键（用于观察竞争者），事务整体返回 `succeeded=False`；
- 任一异常路径都立即 `_revoke_lease_silent` 释放租约，避免泄露。

这里的 `uuid = uuid.uuid1().bytes` 是 owner 标识，理论上可用于「安全释放」(仅当 key 当前值等于自己持有的 uuid 时才删除)，当前实现保留这一信息便于后续扩展。

### 续约与故障感知

EtcdClient 把锁抽象成 `acquire_lock / renew_lease / release_lock` 三段式 API (`etcd_client.py:115-183`)，并在内部维护 `_leases: dict[str, int]` 跟踪本进程持有的租约 ID。

`renew_lease` (`etcd_client.py:137-165`) 调 `LeaseKeepAlive` 流式 RPC 时有两个易错点：

1. **必须消费响应流**：注释 (`etcd_client.py:152-154`) 明确指出，不调用 `next(response_stream)` 会导致请求被缓冲而不真正发出。Motor 直接 `next()` 一次以确保至少一个应答到达。
2. **TTL<=0 即视为失效**：`new_ttl <= 0` 触发 `RuntimeError`，由 `acquire_lock` 外层 `except` 捕获并自动释放锁 (`etcd_client.py:159-165`)，避免锁状态僵死。

`release_lock` (`etcd_client.py:167-183`) 的设计权衡是：即使 `LeaseRevoke` RPC 失败也强制从本地字典 `_leases` 删除，防止客户端误以为仍持有锁。这是一种 "fail-open" 倾向——优先让业务能继续运行，把锁状态交给 etcd 的 TTL 自然失效兜底。

## 三、lock_context：上下文管理器自动管理生命周期

为避免业务代码反复 try/finally，`etcd_client.py:264-276` 提供 `lock_context` 装饰器风格接口：

```python
with client.lock_context("persist_state", ttl=30):
    ...
```

异常路径 (`finally`) 会在 `lease_id` 已获得时调用 `release_lock`，未获得则直接放过。这种「锁即资源」的模式让持久化代码能原子地完成「读-改-写」。

## 四、PersistentState：带校验和的版本化快照

`motor/common/etcd/persistent_state.py:21-64` 的 `PersistentState` 不是另一个 etcd 客户端，而是用于「在 etcd 之上做一次完整的快照 + 完整性校验」的内存数据结构。它通过 pydantic BaseModel 序列化，包含四个字段：

- `data`：要持久化的字典
- `version`：单调递增的版本号
- `timestamp`：生成时间戳
- `checksum`：基于 `data + version + timestamp` 的 SHA-256 摘要

`is_valid()` (`persistent_state.py:33-51`) 在恢复时重新计算 `calculate_checksum()` 并比对：

```python
data_str = f"{str(list(self.data.items()))}{self.version}{self.timestamp}"
hashlib.sha256(data_str.encode()).hexdigest()
```

这种轻量校验能捕获 etcd 中间件异常、数据被外部篡改、版本错位等场景，但**无法防御 etcd 集群本身被整体替换**——它要求调用方在恢复快照后用业务语义再校验，例如读两次比对。

## 五、键空间设计与典型场景

把锁、快照、KV 三类交互落到具体路径上，常见的命名约定为：

| 用途 | 路径模式 | 写入方式 |
|------|---------|----------|
| Controller 主备锁 | `/locks/master_controller` | `lock_context` 30s TTL |
| 实例注册 | `/instances/<id>` | `put_json` + lease |
| 全局快照 | `/snapshot/state` + `/snapshot/state.checksum` | `persist_data` |
| Boolean 标志 | `/flags/scale_p2d_enabled` | `set_bool` |

`delete_prefix` (`etcd_client.py:202-215`) 通过把 prefix 的最后一个字节 +1 作为 `range_end`，实现「删除一个目录下所有键」的语义——这是 etcd v3 区间删除的标准做法，避免了逐键删除的竞态。

## 六、与本目录其他模块的关系

- **与 Controller/Coordinator 配置**：`ControllerConfig.etcd_config` / `CoordinatorConfig.etcd_config` (`motor/config/controller.py:145`, `motor/config/coordinator.py:365`) 持有 etcd 连接参数，作为构造 `EtcdClient` 的入参。
- **与 Standby 主备**：`motor/common/etcd/etcd_client.py` 提供的 `lock_context` 是 `StandbyConfig.master_lock_key` 背后实际抢锁的工具——见 T4 FaultTolerance 的主备切换实现。
- **与 PortAllocator/Config**：本模块是 Motor 与外部共享存储的唯一桥梁，而本目录其他模块（HTTP、Logger、Alarm）都不直接依赖 etcd；它们之间通过 `EtcdClient` 间接解耦。