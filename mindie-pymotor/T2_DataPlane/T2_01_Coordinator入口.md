# T2_01_Coordinator入口

Coordinator 的对外接口分为三个相互独立的 FastAPI 进程平面：`InferenceServer`、`ManagementServer`、`ObservabilityServer`。它们共享 `AppBuilder` 提供的应用骨架，但绑定不同端口、装配不同的中间件、面向不同的客户端。本文梳理入口的职责划分、FastAPI 应用注册方式与 OpenAI 兼容接口的实现要点。

## 三个平面与进程模型

Coordinator 进程由 `CoordinatorDaemon` 派生三类子进程：

- **Mgmt 进程**：仅运行 `ManagementServer`（`motor/coordinator/api_server/management_server.py:76`），对外提供 `/liveness`、`/readiness`、`/startup`、`POST /instances/refresh`、`/` 自描述页面（`management_server.py:230-359`），承担健康探针与实例刷新入口。Mgmt 进程不启动推理 Worker，因此不创建 inference app。
- **Worker 进程**：仅运行 `InferenceServer`（`motor/coordinator/api_server/inference_server.py:120`），对外暴露 `/v1/completions`、`/v1/chat/completions`、`/v1/messages`、`/v1/messages/count_tokens`、`/v1/models` 等 OpenAI/Anthropic 兼容端点（`inference_server.py:353-408`）。Worker 由 `InferenceProcessManager` 通过 `run_inference_worker_proc` 调用 uvicorn 启动。
- **Observability 进程**：仅运行 `ObservabilityServer`，负责 `/metrics`、`/health`、请求遥测。

三个平面共用同一个 `CoordinatorConfig`，但各自只构造自己关心的 TLS、API Key、限流子集。

## FastAPI 应用骨架

`AppBuilder`（`motor/coordinator/api_server/app_builder.py:37`）不注册业务路由，只创建带 CORS 中间件的 FastAPI 壳子，标题与描述按平面定制：

```python
# app_builder.py:46-89
@staticmethod
def create_inference_app(lifespan=None) -> FastAPI:
    app = FastAPI(
        title="Motor Coordinator Inference Server",
        description="Inference API endpoints (OpenAI-compatible and more)",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.add_middleware(CORSMiddleware, **_CORS_CONFIG)
    return app
```

`_CORS_CONFIG` 允许所有来源、方法与 Header，便于在网关后端透传任意客户端。ManagementServer、InferenceServer 在 `__init__` 里调用对应的 `create_management_app`/`create_inference_app` 拿到 app 后再 `_register_routes()` 注入业务端点。

`BaseCoordinatorServer` 提供 `apply_timeout_to_config`、`create_base_uvicorn_config` 等通用方法，处理 Pydantic v2 与 uvicorn 配置差异。ManagementServer 与 InferenceServer 都继承自该基类。

## InferenceServer 路由注册

`InferenceServer._register_routes()`（`inference_server.py:353`）注册了五个端点：

| 路径 | 方法 | 说明 |
|------|------|------|
| `/v1/completions` | POST | 旧式 completions |
| `/v1/chat/completions` | POST | OpenAI chat completions |
| `/v1/messages` | POST | Anthropic messages |
| `/v1/messages/count_tokens` | POST | Anthropic 风格 token 计数 |
| `/v1/models` | GET | 当前可服务的模型清单 |

每个 POST 端点都使用 `@self.timeout_handler()` 装饰器（`BaseCoordinatorServer.timeout_handler`），将整体请求超时与 `infer_timeout` 关联；`request_manager` 通过 `Depends(get_request_manager)`（`inference_server.py:45-47`）从 `app.state.request_manager` 注入。

`_handle_openai_request`（`inference_server.py:458-486`）和 `_handle_anthropic_request`（`inference_server.py:426-456`）的流程一致：

1. 读取 body 并 JSON 解析；
2. 调用 `_validate_openai_request`/`_validate_anthropic_request` 做字段必填校验；
3. 通过 `_is_available()` 调用 SchedulerClient 的 `has_required_instances()` 判定集群是否就绪，否则返回 503；
4. 调用 `dispatch.handle_request()`（`motor/coordinator/router/dispatch.py:150`）进入路由层。

`_validate_openai_request`（`inference_server.py:77-117`）强制 `model` 字段存在、`messages`/`prompt` 至少一个、`messages` 中每条都必须含 `role`+`content`、`role` 必须是 `system/user/assistant/tool`。`_validate_anthropic_request`（`inference_server.py:50-74`）强制 `model` 与 `messages` 必填且 `max_tokens` 为正整数（`count_tokens` 端点不要求 `max_tokens`）。

`/v1/models`（`inference_server.py:400-408`）通过 `_build_models_metadata()` 动态填充 P/D 实例数量：调用 SchedulerClient 分别拉取 `ROLE_P`/`ROLE_D` 的可用实例数，写入 `p_instances_num`、`d_instances_num` 字段（`inference_server.py:410-424`）。

## ManagementServer 路由注册

`ManagementServer._register_routes()`（`management_server.py:230`）注册四个管理端点：

- `GET /startup`：进程启动阶段返回 200，供 Kubernetes `startupProbe` 使用。
- `GET /liveness`：通过 `LivenessProbe`（基于 `RoleShmDaemonLivenessProvider`）判定 Coordinator Daemon 进程是否存活；当 Daemon 已退出或心跳陈旧时返回 503，触发 pod restart（`management_server.py:236-256`）。
- `GET /readiness`：通过 `ReadinessProbe` 判断 Mgmt 是否可以承接流量，区分 `OK_MASTER`、standby、`DAEMON_EXITED`、`HEARTBEAT_STALE`、`NOT_MASTER` 等状态；只有在未就绪时返回 503，避免从 K8s Service 中摘除（`management_server.py:258-324`）。
- `POST /instances/refresh`：Controller 通过该端点将 instance 增删事件投递给 Coordinator；body 校验为 `InsEventMsg` schema，处理路径会同时调用 `SchedulerConnectionManager.refresh_instances` 与本地 `InstanceManager.refresh_instances`（`management_server.py:326-425`）。

`_handle_refresh_instances`（`management_server.py:361-425`）显式做了三层防护：10MB body 上限、空 body 与 JSON 解析失败均返回 400；`InsEventMsg` 反序列化失败时记录 body 字段名便于排障。

`/startup`、`/liveness`、`/readiness` 之所以不放在 Worker 进程，是因为健康探针必须独立于推理 Worker 的生命周期——Worker 重启不应让 readiness 抖动。

## 中间件与安全策略

InferenceServer 在构造完成后可选启用两类中间件：

- **API Key 鉴权**：`verify_api_key(request)`（`inference_server.py:216-237`）从 `Authorization` Header 取 key，支持 `key_prefix` 前缀剥离（典型为 `Bearer `），并在 `_api_key_config.skip_paths` 中放行 `/health`、`/metrics` 等路径。
- **限流**：`setup_rate_limiting()`（`inference_server.py:239-259`）根据配置选择 `olc`（外置限流组件）或自研 `SimpleRateLimitMiddleware`（`inference_server.py:261-283`）；OLC 路径会按 `URL/Method/IP` 维度提取 tag。

ManagementServer 的 `mgmt_ssl_config` 与 InferenceServer 的 `infer_tls_config` 相互独立，可在同一 Coordinator 部署中对外暴露不同 TLS 证书。

## 跨文档引用

- 路由层如何选择 `UnifiedPDRouter`/`PDHybridRouter` 见 `T2_02_Router路由策略.md`。
- 调度器与 `SchedulingFacade` 注入如何被 BaseRouter 消费见 `T2_03_Scheduler调度器.md`。
- Controller 通过 `/instances/refresh` 推送事件的过程见 `T3_03_EventPusher.md`。
- ManagementServer 与 InferenceServer 的进程派生由 `CoordinatorDaemon` 完成，整体进程模型见 `T1_Overview` 中的进程拓扑说明。
