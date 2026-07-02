# T8_01 PDRole 状态语义

PDRole 是 MindIE PyMotor 中描述「一个实例在推理流水线里扮演什么角色」的核心枚举。本节深挖四种角色的语义差异、为什么 `ROLE_E`（Encode）需要单独存在、以及角色在 `Instance` 生命周期中如何流转。基础介绍见 `T1_02_PD分离原理` 与 `T1_05_关键概念一览`，本节只关注状态语义层面的细节。

## 枚举定义与字面量

`motor/common/resources/instance.py:L41-L55`：

```
class PDRole(str, Enum):
    ROLE_E = "encode"
    ROLE_P = "prefill"
    ROLE_D = "decode"
    ROLE_U = "union"
```

`str, Enum` 的混合继承使得 PDRole 值可以直接被 JSON 序列化（值为字符串 `"encode"` / `"prefill"` / `"decode"` / `"union"`），并兼容历史字面量。`_missing_`（L50-L55）把字面量 `"both"` 映射到 `ROLE_U`，保留对历史混合模式的兼容——这是迁移期常见的渐进兼容做法。

## ROLE_E 与 ROLE_P 的关系：为什么 Encode 不是 Prefill 的一种

直觉上 Encode 似乎是「特殊形式的 Prefill」——它也接收输入、产出 token 和 hidden state。但 PyMotor 把它单列为一种角色，是因为二者在工程语义上有三个关键差异：

1. **触发条件不同**：Prefill 是每条 chat/completions 请求都必须经过的阶段；Encode 只在请求携带多模态消息（`image_url` / `video_url`）时才走。`BaseRouter._check_can_encode`（`motor/coordinator/router/strategies/base.py:L671-L699`）会在请求入口判断是否需要 Encode：

   ```
   if not any multimodal content: return False
   if instance_readiness not in {REQUIRED_MET_EPD, ENCODE_PREFILL}: return False
   return True
   ```

   这意味着 Encode 是「按需出现」的角色，而 Prefill 永远出现。

2. **资源画像不同**：Prefill 是计算密集（matrix-heavy），Encode 通常涉及视觉/视频模型的图像预处理和卷积子网络——计算量不一定大但显存占用高，且输入尺寸差异巨大。把 Encode 单独成角色可以让 Encode 实例和 Prefill 实例独立扩缩容，避免「为了多模态请求把 Prefill 实例批量扩起来却只用了一半算力」。

3. **协同位置不同**：Prefill 的产物是 KV Cache，会被 Decode 消费；Encode 的产物是「前序 token」，会进入 Prefill 当作文本输入的一部分。`do_encode`（同文件 L612-L669）在拿到 Encode 响应后释放 Encode 实例的资源，再走正常的 Prefill/Decode 调度——也就是说，Encode 不参与 P/D 的 KV 传输链路，它只是为 Prefill 准备输入。

由此，`REQUIRED_MET_EPD`（既有 Encode 又有 Prefill 又有 Decode）和 `ENCODE_PREFILL`（只有 Encode+Prefill、暂时没有 Decode）才会被作为 readiness 的判定值；只有 Prefill+Decode 没有 Encode 时是 `REQUIRED_MET`，对应纯文本推理。

## ROLE_U：Union 模式与降级路径

`ROLE_U = "union"` 是单实例同时承担 Prefill 与 Decode 的角色，对应 PD 混部模式。它的存在有两个工程意义：

1. **节点资源受限时保留部署能力**：当集群节点不足以为每个角色单独开实例时，可以用 `ROLE_U` 实例跑完整推理。
2. **PD 降级路径**：当 Coordinator 探测到只剩 Prefill 实例可用时，会自动回退到 `PDHybridRouter`（`motor/coordinator/router/strategies/pd_hybrid.py`）并要求 Prefill 实例独立完成 Prefill+Decode。`readiness_from_instances`（`motor/coordinator/domain/scheduling.py:L77-L78`）在 `union_instances` 存在时返回 `REQUIRED_MET`，让请求继续被处理。

`PDHybridRouter._resolve_candidate_roles`（同文件 L53-L71）显式编码了这一降级优先级：

```
if ROLE_U in roles: use ROLE_U
elif ROLE_P in roles: use ROLE_P (single-node fallback)
else: 503
```

这一降级路径对业务方透明——客户端拿到的响应仍然是 OpenAI 兼容格式，只是延迟会显著增加（因为 Prefill 和 Decode 在同一实例上互相干扰）。

## 角色流转：从 Controller 到 Coordinator

角色在系统中的流转路径：

1. **NodeManager 注册**：`EngineManager._register()`（`motor/node_manager/core/engine_manager.py`）向 Controller 发送 `RegisterMsg`，但**不携带 role**——role 由 Controller 决定。
2. **Controller 分配**：Controller 的 `InstanceAssembler` 创建 `Instance`，调用 `InstanceAssembler._send_start_command` 时把 role 写入 `StartCmdMsg`（在 `motor/controller/core/instance_assembler.py` 中可见）。这一步把 role 落到 `Instance.role`（`Instance.role: str = Field(...)`，`motor/common/resources/instance.py:L151`）。
3. **Instance.role 同步给 Coordinator**：Controller 通过 `EventPusher` 把实例状态（包括 role）推送给 Coordinator；Coordinator 端的 `InstanceManager` 接收后写入本地视图。
4. **Coordinator 调度**：`InstanceReadiness` 计算时调用 `instance.role`，Router 据此决定走 `UnifiedPDRouter` 还是 `PDHybridRouter`。

注意 `Instance.role` 的类型是 `str` 而非 `PDRole`（同文件 L151），这是为了在跨进程 JSON 序列化时不必强制序列化 Enum；运行时通过 `PDRole(role_value)` 还原（见 `motor/coordinator/router/strategies/base.py:L813-L824` 的 `PDRole(resource.instance.role)`）。

## 角色与 dispatch_capabilities 的协同

单纯的角色不足以决定 P/D 协同方式——还需要 `Instance.dispatch_capabilities`（同文件 L146-L149）字段。这是 NodeManager 从 `kv_transfer_config.kv_connector` 推导出的「这个实例支持哪些 P/D 协同模式」（`concurrent_engine_sync` / `prefill_handoff_decode`）。`readiness_from_instances` 中 `has_compatible_pd` 判断依据就是 P/D 两端 dispatch_capabilities 的交集：

- 没有共同 capability → P/D 不兼容，`readiness_from_instances` 走「UNKNOWN」分支，路由返回 503（fail-closed）。
- 有共同 capability → 进一步按角色组合分类为 `REQUIRED_MET` 或 `REQUIRED_MET_EPD`。

这是 PDRole 与 dispatch_capabilities 必须**同时携带**才能正确决策的原因——只看 role 会忽略 connector 兼容性。

## 状态机的边界

PDRole 不是「状态」，而是「身份」。一个实例的 role 一旦由 Controller 分配，**通常不再变化**——角色变化发生在以下两种情况：

- **Pod 重启 + 重新组装**：NodeManager 重启后会重新注册，Controller 重新组装实例，role 重新分配。这是一条「先销毁再重建」的路径，不是 role 直接迁移。
- **缩P保D（Scale P2D）**：当 Decode 故障触发 `ScaleP2DStrategy`（`motor/controller/fault_tolerance/strategy/scale_p2d.py`）时，Controller 会释放一个 Prefill 实例的节点用于拉起新的 Decode 实例。释放的 Prefill 实例 `Instance.role` 不变，但会在新 Decode 就绪前出现 `ONLY_PREFILL` 就绪状态——业务上对应自动回退到 `SINGLE_NODE` 模式。

角色身份稳定的好处是：Router / Scheduler 的所有决策都可以假设 role 不变，避免引入「角色切换时的中间状态机」带来的复杂度。角色变化的语义被推到 Pod 重启或缩P保D这种集群级事件里，由 Controller 统一处理。

PDRole 与 InstanceReadiness 的映射、dispatch_capabilities 的推导细节分别在 `T3_01_InstanceManager与状态机` 和 `T8_05_多模型并行支持` 中进一步展开。