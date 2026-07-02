# T7.3 Docker 多容器：HCCL、KV 传输与 Mooncake 配置

## 多容器协作的总览

在 MindIE PyMotor 的 K8s 拓扑中，一个推理集群由多个角色容器组成：Controller、Coordinator、若干 Prefill（P）Server Pod、若干 Decode（D）Server Pod；PD 混部模式下 Prefill 与 Decode 合并为 union 角色。它们通过 K8s Service、环境变量、ConfigMap 三类机制相互发现，部署工具则在 Pod 内通过 `boot.sh` 注入 HCCL/Mooncake 等通信配置。`startup/boot.sh` 的拆解详见 [T6.2 启动脚本与 HCCL 工具](T6_Infrastructure/T6_02_启动脚本与HCCL工具.md)，本节聚焦容器层面的关键配置：HCCL、Mooncake、多容器协同。

## HCCL 通信配置

集合通信库 HCCL 是 NPU 卡间互联的基石。`env.json` 中的 `motor_engine_prefill_env` / `motor_engine_decode_env`（混部场景下 `motor_engine_union_env`）共同决定 HCCL 的运行时行为，常用键如下：

```json
{
  "motor_engine_prefill_env": {
    "HCCL_BUFFSIZE": 200,
    "PYTORCH_NPU_ALLOC_CONF": "expandable_segments:True",
    "HCCL_OP_EXPANSION_MODE": "AIV",
    "OMP_PROC_BIND": "false",
    "OMP_NUM_THREADS": 100,
    "ASCEND_BUFFER_POOL": "4:8"
  }
}
```

- `HCCL_BUFFSIZE` 控制 HCCL 通信缓冲区大小，按卡数与并行度调整；在跨节点 PD 分离场景下若出现通信超时，可适度上调。
- `HCCL_OP_EXPANSION_MODE=AIV` 把集合通信算子展开到 AIV（Vector）核上，与默认 AICore 展开相比，可在保持带宽的同时释放算力。
- `ASCEND_BUFFER_POOL` 是 Ascend 缓冲区池配置，按 `min:max` 形式给出，建议在 `examples/deployer/yaml_template/infer_service_template.yaml`（CRD 场景）或 `engine_template.yaml`（multi_deployment 场景）中保留与硬件匹配的默认值。
- `PYTORCH_NPU_ALLOC_CONF=expandable_segments:True` 启用段扩展策略，降低 NPU 显存碎片。
- `OMP_PROC_BIND=false` 与 `OMP_NUM_THREADS=100` 解除 OpenMP 线程强绑定并给定线程数上限，避免 HCCL 与算子线程互相抢占。

`startup/hccl_tools.py` 会在 Pod 启动阶段根据 `device_num` 与并行配置生成 ranktable，写入容器内的 `/home/HCCL_ranktable.json`（或类似路径），由 vLLM/SGLang 通过 `--custom-ranktable-path` 读取，从而保证跨节点/跨 Pod 的 HCCL 通信拓扑与部署意图一致。

## Mooncake 配置生成器

PD 分离的核心是 KV Cache 在 P 与 D 之间的传输。`examples/deployer/startup/mooncake_config.py` 是一个独立的 Python 脚本，作为容器启动阶段生成 Mooncake 元数据的小工具。其入口支持两种模式：

```bash
python3 mooncake_config.py pool       <output_config_path> <user_config_path>
python3 mooncake_config.py conductor <output_config_path> <user_config_path>
```

### pool 模式（KV 缓存池）

读取 `user_config.json` 根节点的 `kv_cache_pool_config`，合并以下字段后写入输出文件：

- `metadata_server`：元数据服务类型，例如 `P2PHANDSHAKE`。
- `protocol`：传输协议，例如 `ascend`。
- `device_name`：设备名，留空表示自动选择。
- `alloc_in_same_node`：是否在同节点分配 KV 段。
- `global_segment_size`：全局段大小，如 `"1GB"`。

脚本通过环境变量 `KVP_MASTER_SERVICE` 拿到 KV Pool Master 的 Service 域名，并把 `master_server_address` 字段格式化为 `host:port`（IPv6 自动加 `[]`）。若 `KVP_MASTER_SERVICE` 未设置，脚本会直接报错并退出，避免后续引擎读到残缺配置。

### conductor 模式（KV 事件总线）

读取 `user_config.json` 的 `kv_conductor_config`。当 `kvevent_instance` 不为空时，conductor 配置中需要包含 `kvevent_instance.mooncake_master.endpoint`，脚本会用 `KVP_MASTER_SERVICE` 与 KV Pool 的 `port`（默认 `50088`）填充该字段，形成 `tcp://<service>:<port>`。模型名通过 `resolve_model_name` 从 `motor_engine_prefill_config` 中取，先按 `sglang` 引擎读 `served-model-name`，其余引擎读 `served_model_name`，都拿不到时再回退到已废弃的 `model_config.model_name`，最终在 conductor 配置中以 `modelname` 键写入。

两个模式都要求 `user_config_path` 存在且为合法 JSON 对象，否则日志会记录 `Expected JSON object at root` 并返回 False，退出码非 0。

## KV 传输与 Connector

`motor_engine_prefill_config.engine_config.kv_transfer_config` 决定 P/D 协同使用的 connector（参考 [T7.1 K8s 部署模式](T7_01_K8s部署模式.md) 与 `docs/zh/user_guide/deployment/k8s/pd_disaggregation_deployment.md`）。NodeManager 会按以下白名单推导 P/D 协同 capability：

| `kv_connector` | capability | 协同行为 |
|----------------|-----------|---------|
| `MooncakeConnectorV1` | `prefill_handoff_decode` | Prefill 完成后交给 Decode |
| `MooncakeHybridConnector` | `prefill_handoff_decode` | 同上 |
| `NixlConnector` | `prefill_handoff_decode` | 同上 |
| `MooncakeLayerwiseConnector` | `concurrent_engine_sync` | P/D 并发执行，引擎层同步 KV |
| `MultiConnector` | 取 `connectors[0]` 递归判定 | 取决于传输层 connector |

不在白名单内的 connector 不会产生 capability，Coordinator 路由会返回 503（fail-closed）。此时可在 `motor_engine_prefill_config` 与 `motor_engine_decode_config` 顶层显式声明 `dispatch_profile`，P/D 两端必须一致：

```json
{
  "motor_engine_prefill_config": {
    "engine_type": "vllm",
    "dispatch_profile": "handoff",
    "engine_config": {
      "kv_transfer_config": {
        "kv_connector": "YourCustomConnector",
        "kv_role": "kv_producer"
      }
    }
  },
  "motor_engine_decode_config": {
    "engine_type": "vllm",
    "dispatch_profile": "handoff",
    "engine_config": {
      "kv_transfer_config": {
        "kv_connector": "YourCustomConnector",
        "kv_role": "kv_consumer"
      }
    }
  }
}
```

`kv_connector_extra_config.prefill.dp_size` / `tp_size` / `decode.dp_size` / `tp_size` 一般无需手动填写，Motor 在拉起服务时会按 `data_parallel_size` / `tensor_parallel_size` 自动刷新。

## 多容器协同部署的关键约束

- **NPU 网口互联**：集群必须具备参数面互联，所有 P/D 节点对应的 NPU 卡端口需在同一个 VLAN，能通过 RoCE 互通。
- **镜像一致性**：Controller、Coordinator、Prefill、Decode Pod 都使用同一个 `motor_deploy_config.image_name`，避免不同镜像版本导致接口不一致。
- **权重路径对齐**：`weight_mount_path` 在宿主机存在且 `engine_config.model` 指向容器内同一路径，否则 Prefill/Decode 会因为找不到权重卡在加载阶段。
- **网络策略**：Operator 创建 Service 时会给每个 role 暴露独立 ClusterIP；客户端请求先打到 Coordinator Service，再由 Coordinator 按 capability 路由到 P/D Pod。
- **环境变量注入**：Controller 与 Coordinator Pod 的 env 通过 `controller.py` / `coordinator.py` 中的 `modify_*_deployment` 写入；Engine Pod 的 env 由 `engine.py` 的 `build_engine_env_items` 注入，包含 `ROLE`、`JOB_NAME`、`CONTROLLER_SERVICE`、`COORDINATOR_SERVICE` 等关键变量。SGLang 引擎会额外注入 `SGLANG_HOST_IP = status.podIP`，便于启动时自动上报节点 IP。
- **A5 机型特殊处理**：`apply_a5_engine_pod_config` 在 `hardware_type` 命中 A5 时启用 `hostNetwork=true`、`dnsPolicy=ClusterFirstWithHostNet`，并追加 hostPath 形式的 `/usr/local/Ascend/driver` 等挂载卷；`apply_a5_workload` 会写入 `huawei.com/schedule_policy` 注解以及 `accelerator-type=850-Atlas-8p-8` 的 nodeSelector，确保 Pod 被调度到对应 NPU 节点。

## 常见问题

- **HCCL 通信超时**：检查 `HCCL_BUFFSIZE`、NPU 端口 VLAN 是否一致、跨节点 ranktable 是否正确生成。
- **Mooncake 无法发现 KV Pool Master**：确认 `KVP_MASTER_SERVICE` 环境变量被正确注入；`kv_pool_config` 中 `metadata_server` 与 `protocol` 需与 Mooncake 版本匹配。
- **PD 路由返回 503**：通常因为 `dispatch_capabilities` 没有交集。先检查 `kv_connector` 是否在白名单；若不在，确认 P/D 两端的 `dispatch_profile` 一致。
- **A5 节点调度失败**：缺少 `apply_a5_workload` 写入的注解或 `accelerator-type` 标签，需在模板侧确认 `hardware_type` 设置为 A5 派生值。

更多内容可参考 [T7.5 常见故障排查](T7_05_常见故障排查.md) 关于实例与节点异常的处理步骤，以及 [T6 基础设施层说明](T6_Infrastructure/) 中关于 HCCL ranktable 生成与 boot.sh 的细节。
