# KVCache 亲和性调度

## 背景

在 Prefill-Decode 分离架构下，Prefill 阶段的核心工作是计算并生成 KV-Cache（Key-Value Cache）。Decode 阶段则需要反复读取这些 KV-Cache 来生成新 token。如果两个请求的 prompt 前缀高度重叠，第二个请求可以直接复用第一个请求已经计算好的 KV-Cache，避免重复计算，从而显著节省算力。

KVCache 亲和性调度（KvCacheAffinityPolicy）的核心目标，就是**在调度时优先将请求路由到已经持有该请求前缀 KV-Cache 的 Prefill 实例**，从而提高 KV-Cache 复用率。

## 策略实现

KvCacheAffinityPolicy 继承自 `BaseSchedulingPolicy`，定义于 `motor/coordinator/scheduler/policy/kv_cache_affinity.py`。其关键设计在于 `select_endpoint_candidates_from_list` 方法（第 53 行起），支持两种亲和性模式：

- **KV_AFFINITY_MODE_UNIFIED（统一模式）**：优先选择持有最长 KV-Cache 前缀的 Prefill 实例。
- **KV_AFFINITY_MODE_LOAD_GATED（负载门控模式）**：在亲和性基础上叠加负载约束，只在负载最低的 Top-N 实例中选择。

```python
# motor/coordinator/scheduler/policy/kv_cache_affinity.py:L52-60
@staticmethod
def select_endpoint_candidates_from_list(
    instances: list[Instance],
    req_info: RequestInfo,
    mode: str = KV_AFFINITY_MODE_UNIFIED,
    overlap_credit: float = 1.0,
    prefill_load_scale: float = 1.0,
    load_weight: float = 1.0,
    load_gate_topn: int = 0,
```

`overlap_credit` 参数控制亲和性得分的权重，`prefill_load_scale` 控制 Prefill 实例负载的缩放因子，两者共同决定最终选择结果。当 `load_gate_topn` 为 0 时，默认取前 2 个负载最低的候选实例（见第 37 行 `_DEFAULT_LOAD_GATE_TOPN = 2`）。

## 调度流程

整体调度流程遵循以下步骤：

1. **候选实例过滤**：从 InstanceProvider 获取所有 Prefill 实例，过滤出符合角色要求的实例列表。
2. **亲和性评分**：根据请求的 prompt token 与各实例已有 KV-Cache 的重叠程度计算得分。
3. **负载门控**（可选）：在负载门控模式下，只在负载最低的 Top-N 实例中应用亲和性选择。
4. **返回候选端点**：返回得分最高的实例及其对应的 Endpoint。

## 配置参数

在 CoordinatorConfig 中可以通过 `kv_affinity_config` 调整策略行为：

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `kv_affinity_mode` | 亲和性模式：`unified` 或 `load_gated` | `unified` |
| `overlap_credit` | 亲和性得分权重 | `1.0` |
| `prefill_load_scale` | Prefill 负载缩放因子 | `1.0` |
| `load_gate_topn` | 负载门控候选数，0 表示默认 2 | `0` |

## 适用场景

KVCache 亲和性调度最适合**多轮对话**和**前缀重复率高的批量推理**场景。当请求的 prompt 共享大量公共前缀（如系统提示词、few-shot 示例）时，复用 KV-Cache 可以跳过大量重复计算。

对于 prompt 前缀彼此独立、重复率低的场景（如随机单轮问答），亲和性调度的收益有限，反而增加调度复杂度。此类场景建议使用 RoundRobinPolicy 或 LoadBalancePolicy（详见 T2_03_3 和 T2_03_4）。

## 与其他策略的关系

KvCacheAffinityPolicy 与 LoadBalancePolicy、RoundRobinPolicy 最终都通过 PolicyFactory（详见 T2_03_1）根据配置选择实例。三者的核心区别在于选择依据：

- KvCacheAffinityPolicy：以 KV-Cache 复用率为核心指标
- LoadBalancePolicy：以实例当前负载为主要指标
- RoundRobinPolicy：以轮询序号为唯一指标，无任何亲和性

调度策略的选择需要结合业务特征决定，也可通过配置在运行期切换。
