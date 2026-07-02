# T3_05_ConfigResolver

`ConfigResolver`（`motor/config/resolver.py`）是 MindIE PyMotor 在 EngineServer 侧的**多源配置归一化层**。它解决三个问题：

1. **多层级配置**：同一个参数可能同时来自配置文件（`engine_config`/`model_config`）和环境变量，优先级需统一；
2. **命名空间差异**：同一含义的参数在不同引擎（vLLM-Ascend、SGLang）里有不同的 key 名（如 `tensor_parallel_size` vs `tp-size`），需要归一化；
3. **命名风格差异**：kebab-case（`max-tokens`）与 snake_case（`max_tokens`）需互通。

本文梳理 `BaseConfigResolver` 的解析优先级、引擎适配器的差异，以及派生字段（如 `local_world_size`）的计算。

## 双重配置源与优先级

`BaseConfigResolver.__init__`（`resolver.py:39-44`）：

```python
def __init__(self, engine_section: dict[str, Any]):
    self._section: dict[str, Any] = normalize_keys(engine_section)
    raw_model = self._section.get("model_config") or {}
    raw_engine = self._section.get("engine_config") or {}
    self._model_cfg: dict[str, Any] = normalize_keys(raw_model)
    self._engine_cfg: dict[str, Any] = normalize_keys(raw_engine)
```

- `engine_config`：新标准配置空间，**优先级最高**；
- `model_config`：遗留配置空间，向后兼容。

`get(key, default=None)`（`resolver.py:62-74`）的查询顺序：

1. `_GENERIC_KEY_VARIANTS.get(key, (key,))` 拿到所有候选 key；
2. 依次在 `engine_cfg` 找（通过 `_get_engine_key(*variants)`）；
3. 找不到再在 `model_cfg` 找；
4. 都没有则返回 `default`。

冲突警告由 `_warn_conflict`（`resolver.py:50-60`）统一处理：仅在同一 key 在两个空间都被定义且值不同时记录一次警告（`_warned_conflict_keys` 类级集合去重），始终采用 `engine_config` 的值。

## Key 规范化

`normalize_keys`（`resolver.py:18-24`）递归把所有 dict key 的 `-` 替换为 `_`：

```python
def normalize_keys(obj):
    if isinstance(obj, dict):
        return {k.replace("-", "_"): normalize_keys(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [normalize_keys(item) for item in item]
    return obj
```

这让调用方统一使用 snake_case 访问，无需关心原始配置文件用了哪种风格。

## 引擎适配器

`BaseConfigResolver` 通过 `_GENERIC_KEY_VARIANTS` 与 `_PARALLEL_KEY_VARIANTS` 两个类字典暴露"key 别名"映射，子类按引擎声明：

### VLLMConfigResolver

```python
# resolver.py:203-220
class VLLMConfigResolver(BaseConfigResolver):
    _GENERIC_KEY_VARIANTS = {
        "model_name":    ("served_model_name", "served-model-name"),
        "model_path":    ("model",),
        "npu_mem_utils": ("gpu_memory_utilization", "gpu-memory-utilization"),
    }
    _PARALLEL_KEY_VARIANTS = {
        "dp_size":        ("data_parallel_size", "data-parallel-size"),
        "tp_size":        ("tensor_parallel_size", "tensor-parallel-size"),
        "pp_size":        ("pipeline_parallel_size", "pipeline-parallel-size"),
        "pcp_size":       ("prefill_context_parallel_size", "prefill-context-parallel-size"),
        "dp_rpc_port":    ("data_parallel_rpc_port", "data-parallel-rpc-port"),
        "enable_ep":      ("enable_expert_parallel", "enable-expert-parallel"),
        "cp_kv_cache_interleave_size": ("cp_kv_cache_interleave_size", "cp-kv-cache-interleave-size"),
    }

    def get_d2d_config(self) -> dict | None:
        """从 model_loader_extra_config 读 D2D 拓扑 {source, listen_port}"""
```

vLLM 的 key 名较长（`data_parallel_size`），`_get_engine_key` 按 tuple 顺序试匹配，找到第一个非 None 即返回。

### SGLangConfigResolver

```python
# resolver.py:256-282
class SGLangConfigResolver(BaseConfigResolver):
    _GENERIC_KEY_VARIANTS = {
        "model_name":    ("served-model-name", "served_model_name"),
        "model_path":    ("model-path", "model"),
        "npu_mem_utils": ("mem-fraction-static", "mem_fraction_static"),
    }
    _PARALLEL_KEY_VARIANTS = {
        "dp_size": ("dp-size", "dp_size"),
        "tp_size": ("tp-size", "tp_size"),
        "pp_size": ("pp-size", "pp_size"),
    }

    def _resolve_engine_parallel_keys(self):
        result = super()._resolve_engine_parallel_keys()
        cp_size = self._get_engine_key("context-parallel-size", "context_parallel_size")
        cp_enabled = self._get_engine_key("enable-prefill-context-parallel", "enable_prefill_context_parallel") or False
        if cp_size and cp_enabled:
            result["pcp_size"] = cp_size
        return result
```

SGLang 多了对 `pcp_size` 的隐式推导：仅当 `cp_size` 与 `enable_prefill_context_parallel` 同时存在时才把 `cp_size` 映射成 `pcp_size`，这是 SGLang 与 vLLM 在并行策略命名上的差异。

## 派生字段计算

`get_parallel_config()`（`resolver.py:107-129`）：

1. 用 `_resolve_engine_parallel_keys()` 拿到引擎原生 key 映射的内部并行字典；
2. 合并 `model_config.parallel_config` 的遗留字段（冲突时按 `_warn_conflict` 报警）；
3. 强制覆盖 `local_world_size = pcp * tp * pp` 与 `world_size = dp * local_world_size`（`resolver.py:131-146`）——这两个字段永远由计算得出，不允许用户配置覆盖。

`_compute_local_world_size` 是可被子类覆盖的钩子（`resolver.py:131`），让不同引擎自行定义 local world size 语义。

## enable_multi_endpoints 解析

`enable_multi_endpoints` 在 vLLM/SGLang 下有不同的获取位置：

- vLLM（`resolver.py:85-89`）：先看 top-level `enable_multi_endpoints`，否则取 `engine_config.enable_multi_endpoints`，默认 True；
- SGLang（`resolver.py:271-272`）：仅取 `engine_config.enable_multi_endpoints`，默认 False——SGLang 默认不启用 multi endpoints。

## ConfigResolver 工厂

`ConfigResolver(engine_section, engine_type=None)`（`resolver.py:290-308`）：

```python
def ConfigResolver(engine_section, engine_type=None):
    if engine_type is None:
        engine_type = engine_section.get("engine_type")
    if not engine_type:
        logger.warning("engine_type not specified, defaulting to vllm")
        engine_type = "vllm"
    if engine_type == "sglang":
        return SGLangConfigResolver(engine_section)
    if engine_type != "vllm":
        logger.warning("unknown engine_type '%s', falling back to vllm", engine_type)
    return VLLMConfigResolver(engine_section)
```

`engine_type` 通常由 `engine_section.engine_type` 提供；缺失或未知时 fallback 到 vLLM，避免静默错配。

## 与 Controller / Coordinator 配置的关系

`ConfigResolver` 主要面向 **EngineServer 进程**——EngineServer 启动时需要把 deploy_config 翻译成具体的 vLLM/SGLang 启动参数。Coordinator / Controller 的配置（`CoordinatorConfig`/`ControllerConfig`）走各自独立的 Pydantic 模型（`motor/config/coordinator.py`、`motor/config/controller.py`），由 `from_json()`/`update_config()` 处理，不经过 `ConfigResolver`。

## 环境变量与 etcd

环境变量的优先级由 `CoordinatorConfig.from_json()`/`ControllerConfig.from_json()` 处理（`USER_CONFIG_PATH` 等）。ETCD 仅用于：

- Controller 端的实例状态持久化（`/controller/instance_manager`，详见 `T3_02_InstanceManager.md`）；
- Conductor 端的 KV 索引（`ConductorApiClient.register_post`）。

`ConfigResolver` 不直接读 ETCD——它是**启动期**配置归一化，运行时通过 ConfigWatcher 热更新（`controller/main.py:283`）。

## 跨文档引用

- EngineServer 如何按 `dispatch_profile` 决定走 handoff 还是 metaserver 路径见 `T2_05_DispatchAdapter.md`。
- KV block size 等 prefill 配置如何被 Conductor 注册逻辑消费见 `motor/coordinator/api_client/conductor_api_client.py`。
- ConfigWatcher 与 `on_config_updated` 的热更新流程见 `T3_01_Controller角色定位.md`。
