# T6_03 Logger 系统：Formatter、压缩轮转与速率限流

Motor 的日志系统由四块组成：格式化器（`formatter.py`）、handler（`logger_handler.py`）、核心入口（`logger.py`）与速率限流（`rate_limited_logger.py`）。它对外暴露 `get_logger(name)` 一个 API，但内部做了不少精细工作：日志桶（bucket）隔离、压缩轮转、调试态相对路径、第三方库噪声抑制、错误聚合等。

## 一、Formatter：NewLineFormatter 与 ColoredFormatter

`motor/common/logger/formatter.py` 定义两种格式化器，二者都继承自 Python 标准库 `logging.Formatter`。

### NewLineFormatter：vLLM 对齐风格

`NewLineFormatter` (`formatter.py:19-46`) 主要做两件事：

1. **注入 fileinfo 字段**：把 `record.filename` 或 `_fileinfo` 计算结果注入 record，供格式字符串中的 `[%(fileinfo)s:%(lineno)d]` 占位符使用；
2. **多行对齐**：当 `record.message` 含换行时，把首行的 `[name][fileinfo:lineno]` 前缀复制到每一行前面——这样在文本查看器里多行日志仍保持字段对齐，肉眼扫读更轻松。

`_fileinfo` (`formatter.py:34-46`) 在 `use_relpath=True` 时把绝对路径缩成「相对项目根 + 省略中间目录」的紧凑形式，逻辑来自 `_shrink_path` (`formatter.py:49-61`)：

- 砍掉开头的 `motor/`，因为所有调用方都在 motor 之下；
- 保留首段（文件名）+ 末尾两段（向上两级目录），中间用 `...` 占位；
- 例：`motor/controller/foo/bar/baz.py` → `baz.py` 在 `foo/.../baz.py`，实际渲染为 `foo/.../bar/baz.py` 这种形式。

`use_relpath` 由 `_build_formatter` 在 DEBUG 模式下开启 (`logger.py:131`)——生产用绝对文件名不缩短（避免歧义），调试用相对路径缩短（避免日志太长）。

### ColoredFormatter：终端彩色

`ColoredFormatter` (`formatter.py:64-96`) 在 NewLineFormatter 基础上对 `levelname` 上色，并给时间戳与位置字段套灰色，便于在 TTY 里区分严重级别。颜色表与 vLLM 的 `ColoredFormatter` 对齐：

```
DEBUG   \033[37m (white)
INFO    \033[32m (green)
WARNING \033[33m (yellow)
ERROR   \033[31m (red)
CRITICAL\033[35m (magenta)
```

颜色由 `_use_color` (`logger.py:119-127`) 判定：尊重 `NO_COLOR` 环境变量；`MOTOR_LOGGING_COLOR=0/1` 强制开关；否则仅在 `sys.stdout.isatty()` 为真时启用。

## 二、MaxLengthFormatter：行长截断

`logger.py:59-72` 的 `MaxLengthFormatter` 是装饰器式封装：

```python
msg = repr(msg)[1:-1]
if len(msg) > self.max_length:
    return msg[: self.max_length] + '...'
```

`repr()[1:-1]` 把字符串先变成 escape 形式再截断，保证截断后的边界不会切断 UTF-8 多字节字符或转义序列。`log_max_line_length` 默认由 `LoggingConfig` 提供，过长的请求体或堆栈不会撑爆日志文件。

## 三、CompressedRotatingFileHandler：时间戳压缩轮转

`motor/common/logger/logger_handler.py:27-293` 的 `CompressedRotatingFileHandler` 是对 `RotatingFileHandler` 的扩展。三个核心机制：

1. **基于大小轮转**：当单文件超过 `maxBytes`（默认 10MB）触发 `doRollover`；
2. **压缩归档**：轮转后的文件重命名为 `<base>_<UTC timestamp>.log`，由独立后台线程读取 `compress_queue` 用 gzip 压缩（默认级别 6）；
3. **总量控制**：`_perform_cleanup` (`logger_handler.py:253-293`) 扫描同目录下所有 `<base>*` 文件，超出 `max_total_size`（默认 200MB）或 `backup_count` 的旧文件入 `cleanup_queue`，由另一个后台线程删除。

后台线程用 `daemon=True` 启动，2 秒与 10 秒轮询周期分别处理压缩与清理。`doRollover` 流程 (`logger_handler.py:65-89`)：

1. 调用父类 `doRollover` 完成标准重命名（`.1, .2, ...`）；
2. 找到刚生成的 `.1` 文件，把它改名带上 UTC 时间戳（`%Y-%m-%d_%H-%M-%S`）；
3. 把路径压入 `compress_queue`；
4. 立即执行一次 `_perform_cleanup`。

`keep_uncompressed_count`（默认 2）保证最新的两份轮转文件不被压缩，方便运维人员 `tail` 查看最近未压缩日志。

## 四、_ensure_shared_handlers：进程级共享 Handler 池

`logger.py:141-229` 实现的关键能力是 **handler 不挂在 root 上，而是挂在每个 motor bucket logger 上**。原因写在 `_attach_shared_handlers` (`logger.py:232-256`) 的 docstring 里：

> Handlers live on the bucket logger, not on root. Propagation is disabled in production to prevent double emission when root accidentally picks up a handler (e.g. from logging.basicConfig() in a third-party lib, an example script, or a side effect during import).

这是 vLLM 同款设计——很多三方库会调用 `logging.basicConfig()` 给 root 加 handler，如果 motor 也把 handler 挂在 root 上，就会双发。Bucket 隔离使得 motor 只输出自己的日志，三方库的 INFO 噪声进不来（除非通过 `httpx, urllib3, httpcore, uvicorn.error` 的 point-kill，见下文）。

### 日志目录推导

`logger.py:178-182` 利用 hostname 形如 `motor-ctrl-7f9c-abcd12` 的格式砍掉最后两段得到 pod 共享目录：

```python
parts = hostname.split('-')
if len(parts) <= 2:
    module_log_dir = os.path.join(log_dir, hostname)
else:
    module_log_dir = os.path.join(log_dir, '-'.join(parts[:-2]))
```

这样多个 Pod 实例（同名不同 suffix）共享一个目录，文件命名 `<hostname>_<pid>.log` 在目录内区分实例。

## 五、_resolve_logger_name：桶名折叠

Motor 收到 `motor.controller.observability.alarm.foo` 这样的长 logger 名后，会按规则折叠成较短的桶名（`_resolve_logger_name`，`logger.py:103-116`）：

- 第一层在 `_TOPLEVEL_COMPONENTS`（`engine_server, node_manager, config`）—— 直接使用顶层名；
- 第二层在 `_SECONDLEVEL_COMPONENTS`（`controller, coordinator, common`）—— 取第三段；
- 其他—— 用第二段+第三段拼接。

目的是让 bucket 数量可控（避免 100+ 个独立 logger 对象），共享 handler 时开销集中。

## 六、_suppress_noisy_third_party_loggers：噪声点杀

`logger.py:296-306` 对四个高频噪声库做点杀：`httpx, httpcore, urllib3, uvicorn.error`。当 motor log_level = INFO 时把这些 logger 强制设为 WARNING；DEBUG 时恢复，让用户能下钻排查。这种「按需开放」的策略与 vLLM 的 noisy logger 处理对齐。

## 七、RateLimitedLogger：日志质量治理

`motor/common/logger/rate_limited_logger.py` 实现两类反垃圾日志机制。文件头 docstring (`rate_limited_logger.py:11-28`) 写得很清晰：

> Two complementary mechanisms:
> 1. ``error_window`` — collapses repeated identical errors inside a time window into a single ``count=N`` line.
> 2. ``emit_periodic`` — emits a periodic summary line for high-frequency success paths.

### error_window：窗口聚合

`error_window` (`rate_limited_logger.py:56-100`) 维护 `key -> {count, first_ts, last_msg}` 的状态字典：

- 第一次出现立即原样输出并记 `count=1`；
- 后续每来一次 `count += 1`；如果窗口已过则输出 `"{msg} (last {N}s saw {K} occurrences)"` 摘要并重置窗口；
- 窗口内增量期间直接抑制（不输出）。

适用场景：网络抖动、RPC 偶发超时——这些日志如果不聚合，会在控制台刷屏，反而淹没真正有用的告警。

### emit_periodic：定期摘要

`emit_periodic` (`rate_limited_logger.py:121-174`) 与 `record_success` (`rate_limited_logger.py:106-119`) 配对：

- `record_success(key)` 累加 `success_count`；
- `emit_periodic(key, msg_template, interval_sec)` 每隔 `interval_sec` 把累计成功次数刷成一条摘要日志；
- `level` 是 sticky 的：首次注册 key 时确定级别，后续 emit_periodic 不论传什么 level 都用首次值，保证 `flush_all` 关闭时的语义一致。

适合「每秒调用一次但每次都打 INFO 会刷屏」的健康探针，比如 coordinator 的 ready 检查。`flush_all` (`rate_limited_logger.py:176-181`) 强制把待发摘要 flush 出去——在进程关闭或测试 teardown 时调用避免丢失最近窗口的数据。

## 八、LoggingConfig 与重配置

`LoggingConfig` 在 `motor/config/log_config.py` 中定义。`reconfigure_logging` (`logger.py:309-346`) 接受新 LoggingConfig，重建共享 handler 并重挂到已知 bucket，触发时机一般是 ConfigMap 变更后由外部 watcher 调用。pytest 环境通过 `PYTEST_CURRENT_TEST` 环境变量跳过重配——这是为了不破坏 `caplog` 的捕获（caplog 钩在 root 上）。

## 九、模块关系

- **与 Config**：`LoggingConfig` 是 `ControllerConfig / CoordinatorConfig / NodeManagerConfig` 三个 dataclass 的第一个成员 (`controller.py:134`, `coordinator.py:352`, `node_manager.py:264`)。
- **与 Alarm**：AlarmStore（T6_06）上报严重事件时也走 motor 自己的 logger，不直接打 stdout——这样日志格式、压缩、轮转策略统一。
- **与 HTTP/Etcd**：HTTP/Etcd 模块的失败日志（SSL 校验失败、租约续约失败）全部走 `logger.debug` 或 `logger.error`，由本系统的 formatter/handler 统一处理。