# T5_05 HealthCollector

## 职责

`HealthCollector`（`motor/engine_server/core/health_collector.py`）是 EngineServer 进程内**主动健康探针**，周期探测推理端口（`/health`）是否返回正常状态。它与 `EngineServerApiClient.query_status`（由 HeartbeatManager 调用）共同构成两层探测：

- `EngineServerApiClient.query_status`：NodeManager 侧通过 mgmt port 拉取状态。
- `HealthCollector`：EngineServer 进程内自检，常用于 readiness / liveness 探针、K8s Service 反向剔除。

## 接口

```python
async def is_healthy(self) -> bool
```

实现位于 `health_collector.py:31-53`：

1. 构造内部 endpoint（`build_endpoint(host, port)`，考虑 IPv6 等格式）。
2. 用 `AsyncSafeHTTPSClient.create_client` 发起 `GET /health`：
   - 首次连接或主机侧快照恢复后尚未刷新地址时，把 `host` 替换为 `get_pod_ip()`，保证恢复后路由正确。
   - `timeout` 取自 `endpoint_config.deploy_config.health_check_config.health_collector_timeout`。
3. 解析响应：响应文本小写化后不等于 `'false'` 即视为健康（`'true'` 与其他非 `'false'` 字符串都通过）。
4. 失败处理：
   - `_has_connected = True` 后再失败 → 返回 `False`，让上层判定异常。
   - 首次连接即失败 → 抛异常，让 readiness probe 标记为 NotReady，由 K8s 决定后续动作。

## 状态字段

- `self._has_connected`：首次成功连接后置 `True`，区分"启动早期尚未建立连接"与"运行期掉线"两种场景。后者直接返回 `False`；前者抛异常让上层重试，避免冷启动期间误判。
- `self._has_refreshed_after_restored`：主机侧快照恢复后只刷新一次 endpoint，避免每次调用都触发 DNS / Pod IP 重算。

## 部署位置

HealthCollector 嵌入在 EngineServer 进程内，由 EngineServer 的 readiness / liveness handler 调用（具体调用方在 `motor/engine_server/core/...` 中的 readiness 模块，与本目录主题互补）。其作用是给 K8s 端到端的健康判断提供依据，配合 HeartbeatManager 上报的 endpoint 状态共同决策是否将实例对外提供服务。

## 与 HeartbeatManager 的关系

两个组件的差异：

| 维度 | HealthCollector | HeartbeatManager._get_engine_server_status |
|------|-----------------|---------------------------------------------|
| 运行进程 | EngineServer | NodeManager |
| 探测目标 | inference `/health` | mgmt port `/status` |
| 调用者 | EngineServer readiness handler | NodeManager 后台轮询线程 |
| 失败语义 | 返回 `False` 或抛异常 | 设置 `endpoint.status = ABNORMAL` |

两者并存使得 EngineServer 与 NodeManager 任一进程崩溃时，Controller 都能在下一轮心跳或下次 readiness 探测中捕获。

## 文档交叉引用

- HeartbeatManager 探测与心跳上报：`T5_03_HeartbeatManager.md`。
- FaultReporter 通过 ZMQ 监听 engine 状态：`T5_01_NodeManager架构.md`、`T4_02_FaultManager.md`。
- K8s readiness/liveness 探针配置：T6 基础设施章节。