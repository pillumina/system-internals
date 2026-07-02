# T7.2 CRD 部署：InferServiceSet 与 Operator

## 设计动机

CRD 部署把 Controller、Coordinator、Prefill、Decode（PD 分离）或 union（PD 混部）等角色统一封装在 `InferServiceSet` 自定义资源中，由 MindCluster 的 infer-operator 监听并负责 pod 生命周期。相比多 YAML Deployment 方式，CRD 部署带来三点收益：扩缩容只需修改 `InferServiceSet.spec.roles[].replicas` 一处；operator 能感知整个推理集群的拓扑并做出更细粒度的调度决策；通过 `schedulingStrategy` 与 role 级 `priority` 字段实现优先级抢占，为 ScaleP2D 等弹性策略提供底层支撑。设计动机、限制与扩缩容流程的完整论述见 `docs/zh/design/crd_deployment.md`。

## InferServiceSet 资源结构

模板文件 `examples/deployer/yaml_template/infer_service_init.yaml` 是多文档 YAML，按顺序包含四类资源：

1. **ServiceAccount**：`mindie-motor-controller`，由 Controller Pod 使用。
2. **ClusterRole**：`mindie-controller-role`，授予对 `configmaps`/`nodes` 的 `get`/`list`/`watch` 权限，便于读取配置与节点拓扑。
3. **ClusterRoleBinding**：`mindie-controller-binding`，把上述 ClusterRole 与 ServiceAccount 在部署命名空间内绑定。
4. **InferServiceSet**：核心资源，声明各角色的 workload、镜像、副本数、调度策略等。

`deploy.py` 在 `generate_yaml_infer_service_set` 中读取该模板并按 `user_config.json` 实例化，主要步骤在 [T7.1 K8s 部署模式](T7_01_K8s部署模式.md) 中已经描述，此处不再重复。需要特别说明的是：在 CRD 部署场景下，`role.services` 不在模板里添加 metadata，由 CRD controller 按命名规则创建 K8s Service；prefill/decode 的 job-name 由 deploy.py 设为 `{namespace}-{InferServiceSet.metadata.name}` 初值，pod 启动后会被 `boot.sh` 改写为 `{namespace}-{InferServiceSet_name}-{INFER_SERVICE_INDEX}-p/d{INSTANCE_INDEX}`，用于后续 endpoint 聚合与调度。

## 部署模式与 ConfigMap 校验

`deploy_mode` 在首次部署时从 `user_config.json` 读取，缺省值为 `infer_service_set`。运行时（即扩缩容、`--update_config`）以集群 ConfigMap `motor-config` 中保存的 baseline 为准：

- **`--update_instance_num`**：调用 `validate_only_instance_changed` 校验，仅允许 `p_instances_num` / `d_instances_num` / `hybrid_instances_num` 三者变化；`deploy_mode` 在 baseline 中存在但 user_config 中被改写时直接报错，避免在运行时切换部署拓扑。
- **`--update_config`**：显式检查 `deploy_mode`，与 baseline 不一致时拒绝刷新 ConfigMap，因为仅靠 ConfigMap 不足以让 operator 把已有 Deployment 转换成 InferServiceSet。

如确需切换部署模式，必须先 `bash delete.sh <namespace>` 删除命名空间，再修改 `user_config.json` 重新全量部署。

## ConfigMap 策略

`create_motor_config_configmap` 把 `user_config.json`、`boot.sh`、`probe` 等文件打包进命名空间下的 `motor-config` ConfigMap，供各 Pod 通过 volumeMount 挂载到 `/mnt/configmap`。无论 `infer_service_set` 还是 `multi_deployment`，这一 ConfigMap 逻辑是共享的，因此 controller 与 coordinator 在切换模式时不需要重新调整挂载路径。运行时可通过修改该 ConfigMap 的内容（结合 `--update_config` 白名单内的字段）实现 Controller/Coordinator/NodeManager 日志等级、超时等参数的热生效。完整白名单见 [T7.6 弹性伸缩配置](T7_06_弹性伸缩配置.md) 以及 `docs/zh/user_guide/deployment/k8s/update_config_whitelist.md`，目前允许的字段主要包括：

- Controller：`logging_config.log_level`、`observability_config.observability_enable`、`observability_config.metrics_ttl`。
- Coordinator：`logging_config.log_level`、`exception_config.{max_retry,retry_delay,first_token_timeout,infer_timeout}`、`timeout_config.{request_timeout,connection_timeout,read_timeout,write_timeout,keep_alive_timeout}`。
- NodeManager：`logging_config.log_level`。

## 优先级调度与实例强制删除（ScaleP2D 前置）

在 PD 分离场景下，若需启用 [T7.5 常见故障排查](T7_05_常见故障排查.md) 中的 ScaleP2D 弹性策略，必须在 `InferServiceSet.spec.template` 下显式开启优先级调度，并在 prefill/decode 的 role spec 中配置 `priority`，同时把 Pod 模板的 `fault-scheduling` 标签从 `grace` 改为 `external-force`：

```yaml
spec:
  template:
    schedulingStrategy:
      type: Priority
    roles:
      - name: prefill
        spec:
          replicas: 2
          priority: 2        # 数值越大优先级越低，更易被抢占
      - name: decode
        spec:
          replicas: 2
          priority: 1        # 数值越小优先级越高，ScaleP2D 不会抢占 decode
        template:
          metadata:
            labels:
              fault-scheduling: external-force   # 原为 grace，ScaleP2D 需要
              fault-retry-times: "10000"
              app: mindie-server
```

ScaleP2D 的工作流会根据 `priority` 顺序选择被抢占的 P 实例，再由 CRD operator 强制回收 Pod、释放节点。具体执行步骤见 [T7.6 弹性伸缩配置](T7_06_弹性伸缩配置.md) 与 `docs/zh/user_guide/features/scale_p2d.md`。

## 常见操作清单

```bash
# 首次部署（CRD 方式，deploy_mode 默认即为 infer_service_set）
python3 deploy.py --config_dir ../infer_engines/vllm

# 扩容：调大 p_instances_num / d_instances_num，再执行
python3 deploy.py --config_dir ../infer_engines/vllm --update_instance_num

# 缩容：调小实例数，执行同样的 --update_instance_num

# 升级 ConfigMap（仅白名单字段）
python3 deploy.py --config_dir ../infer_engines/vllm --update_config

# 卸载
bash delete.sh <namespace>
```

## 限制与避坑

- **CRD 方式尚未完成 RAS 能力与池化能力的适配验证**：若业务需要可靠性、可用性、可服务性或 KV 池化，请使用 `multi_deployment`。
- **infer-operator 必须存在**：首次部署前确认集群已安装 MindCluster 的 infer-operator，否则 `kubectl apply` 之后 InferServiceSet 会停留在 Pending 状态。
- **不允许在 update 场景切换 deploy_mode**：若误改 `deploy_mode`，请先 `bash delete.sh` 再重新部署。
- **混部 / 分离的角色副本数约束**：混部模式下 `prefill.replicas` 与 `decode.replicas` 均为 0，全部由 `union.replicas`（即 `hybrid_instances_num`）承担；扩缩容只能修改 `hybrid_instances_num`。

更多上下文请参考 [T7.1 K8s 部署模式](T7_01_K8s部署模式.md) 关于 deploy.py 入口与生成器的拆解，以及 [T7.6 弹性伸缩配置](T7_06_弹性伸缩配置.md) 关于 ScaleP2D 配置项的语义说明。

## 核心代码路径索引

CRD 部署的逻辑由 deploy.py 驱动，以下是关键路径与对应的源码位置：

| 操作 | 代码位置 | 关键逻辑 |
|------|---------|---------|
| infer_service_init.yaml 渲染 | `examples/deployer/deploy.py:generate_yaml_infer_service_set` | 读取多文档 YAML，按 user_config 实例化 |
| 模板填充字段映射 | `examples/deployer/lib/generator/engine.py:apply_node_selector_by_hardware` | hardware_type → nodeSelector key |
| A5 hostNetwork 配置 | `examples/deployer/lib/generator/engine.py:apply_a5_engine_pod_config` | 写入 hostNetwork / hostPath 卷 |
| priority 字段注入 | `InferServiceSet` CRD spec.roles[].spec.priority | 模板中预留占位符，deploy.py 写入 |
| fault-scheduling 标签 | K8s Pod template metadata.labels | external-force 标签触发 CRD operator 强制回收 |
| ConfigMap 创建 | `examples/deployer/deploy.py:create_motor_config_configmap` | motor-config ConfigMap 的 data 字段组装 |

CRD operator（MindCluster infer-operator）监听 InferServiceSet 资源变化后，按 `spec.roles` 生成对应的 Deployment、Service、Pod 等原生资源。其中 `spec.template.spec.schedulingStrategy.type: Priority` 开启优先级调度后，operator 会根据 `priority` 值决定当资源不足时先驱逐哪些 Pod——这正是 ScaleP2D 能够抢占 P 实例并释放节点资源的底层机制，详见 `docs/zh/user_guide/features/scale_p2d.md`。
