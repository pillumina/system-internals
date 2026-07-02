# T6_02 HTTP 客户端：连接池、TLS 握手与安全工具

Motor 内部各组件之间的 HTTP 通信几乎都经过 `motor/common/http/` 下三个文件协作完成：同步 `SafeHTTPSClient`、异步 `HTTPClientPool`、`CertUtil` 证书工具和 `security_utils` 安全工具。本节梳理它们如何做到「连接复用 + TLS 严格校验 + 失败语义清晰」。

## 一、SafeHTTPSClient：同步客户端与短/长连接

`motor/common/http/http_client.py:48-178` 的 `SafeHTTPSClient` 是同步场景下的入口。设计上分三步：

1. **协议判定**（`http_client.py:62-72`）：若 `tls_config.enable_tls` 为真，强制使用 `https://` 并通过 `CertUtil.create_ssl_context` 构建 SSL 上下文（`Purpose.CLIENT_AUTH`），挂到 `requests.HTTPAdapter` 上。HTTPAdapter 的连接池大小硬编码为 10 (`connections=10, maxsize=10`)——这是单实例同步客户端的合理上限，超出后请求会阻塞等待连接。
2. **基础 URL 拼接**（`http_client.py:73`）：`_normalize_address` 把 IPv6 字面量加上方括号（`split_address` + `format_address`），避免 Python socket 解析失败。
3. **Header 预设**（`http_client.py:76-84`）：默认带 `User-Agent: Secure-HTTPS-Client/1.0` 与 `Accept: application/json`，并通过 `Connection` 头部在 `ConnectionMode.SHORT`（默认）与 `LONG` 之间切换。

`ConnectionMode` 枚举 (`http_client.py:43-45`) 表达的是 HTTP keep-alive 语义：SHORT 把 `Connection: close` 写到请求头，每次请求结束都关闭底层 TCP；LONG 则交给池化层复用。

### 请求方法与错误归类

`request / get / post` 系列方法 (`http_client.py:100-121`) 全部走统一的 `_request` (`http_client.py:127-178`)。它在失败处理上做了精细分类：

| 异常 | 抛出的 `RuntimeError` | 日志要点 |
|------|----------------------|----------|
| `requests.exceptions.SSLError` | `SSL verify failed: ...` | 提示 CA 不匹配/过期/域名不匹配 |
| `requests.exceptions.HTTPError` | `http response error {code}, {body}` | 提示服务端拒绝/宕机/鉴权失败 |
| 其他 `Exception` | `send request {url} error: ...` | 提示连接拒绝/网络不可达/DNS 失败 |

日志用 `logger.debug` 而非 `error`——因为 `raise_for_status` 已经把 HTTP 状态码异常抛给调用方；这里把日志级别留给业务侧根据上下文再判断。这种「异常携带足够上下文 + 日志只做诊断线索」的模式让上层 try/except 只需关心「下一步重试还是失败」。

## 二、HTTPClientPool：异步客户端池与双检锁缓存

异步场景下重建 `httpx.AsyncClient` 的代价较高，Motor 在 `HTTPClientPool` (`http_client.py:229-401`) 中实现进程内单例缓存。关键设计：

### 池键生成

`_get_pool_key` (`http_client.py:350-362`) 把 `format_address(ip, port)` 与 TLS 配置哈希拼接：

```python
tls_str = f"{tls_config.enable_tls}_{tls_config.ca_file}_{tls_config.cert_file}_{tls_config.key_file}"
tls_hash = hashlib.md5(tls_str.encode(), usedforsecurity=False).hexdigest()[:8]
```

`usedforsecurity=False` 显式声明这是非安全用途，避免 OpenSSL 在 FIPS 模式下拒绝 MD5。同时 `_tls_hash_cache: dict[int, str]` 用 `id(tls_config)` 作为键缓存哈希——同一个 TLSConfig 对象反复传入时不会重复计算。

### 双检锁 + 异步安全关闭

`get_client` (`http_client.py:244-279`) 是经典 DCL 模式：

```python
client = self._client_pool.get(pool_key)
if client and not client.is_closed:
    return client
with self._lock:
    client = self._client_pool.get(pool_key)
    if client and not client.is_closed:
        return client
    # 真正创建新客户端
    ...
```

注意一个细节：老客户端的关闭动作放在锁外 (`await self._safe_aclose(old_client_to_close)`)，避免在持锁时调用异步 IO——这是异步代码与同步锁协作时最容易踩坑的地方。

### 批量预热与失效清理

`warmup_clients` (`http_client.py:296-321`) 用 `asyncio.gather` 并发创建一组客户端，常用于 Controller 启动时预先建连所有 EngineServer 的管理端口，缩短首次请求延迟。`cleanup_unused_clients` (`http_client.py:331-348`) 反向清理：传入当前活跃的 pool_key 集合，未在集合中的客户端先 `cancel_all` 再 `aclose`，回收资源。

`cancel_all` 与 `_cancellers` (`http_client.py:181-198`) 是故障域传播的关键：当一个 endpoint 故障需要取消其上挂起的所有请求时，调用方注册 canceller；故障发生时 `cancel_all` 用统一的 `NODE_FAULT` reason 触发所有挂起协程退出。这种「资源 + 取消器」配对模式让 fault tolerance 模块能精确传播故障边界。

## 三、CertUtil：证书链严格校验

TLS 在分布式系统里出问题最多的是「证书改了一行忘了同步」，Motor 因此在 `motor/common/http/cert_util.py` 实现了相对严格的预校验：

### 服务器证书校验

`validate_server_certs` (`cert_util.py:124-201`) 五步检查：

1. **版本强制 X509v3**：`server_cert.get_version() != 2` 直接拒绝；
2. **签名算法白名单**：仅允许 `sha256WithRSAEncryption / sha384WithRSAEncryption / sha512WithRSAEncryption` (`cert_util.py:137-142`)；
3. **RSA 长度 ≥ 3072**：`MIN_RSA_LENGTH = 3072` (`cert_util.py:36`)，否则拒绝；
4. **扩展用途检查**：CA 标志、key_cert_sign、crl_sign 任一为真则提示证书可能不是 End Entity 证书——这是「防止误把 CA 证书当成 server 证书用」的护栏；
5. **有效期检查**：`has_expired` (`cert_util.py:101-121`) 用 UTC 时间比对 `notBefore / notAfter`，避免本地时区差异导致误判。

### 证书-私钥匹配

`validate_certs_and_keys_modulus` (`cert_util.py:46-58`) 通过比较证书和私钥的 RSA 模数 `n` 判定是否成对——这是检测「证书换了但忘了换私钥」的最直接手段。

`validate_cert_signature` (`cert_util.py:61-98`) 不仅 `verify_directly_issued_by`，还比较 issuer 与 CA subject 的 hash，确保整条链可信。

### CA 与 CRL

`validate_ca_certs` (`cert_util.py:282-393`) 对 CA 证书做完整校验：版本、CA flag、digital_signature、key_cert_sign、crl_sign、签名算法、RSA 长度、有效期。`validate_revoke_list` (`cert_util.py:245-279`) 加载 PEM CRL 后检查 `last_update_utc <= now < next_update_utc`，确保 CRL 文件本身是当前有效的。

`validate_ca_crl` (`cert_util.py:553-583`) 用 CA 的公钥验证 CRL 签名——防止「CRL 文件被替换为伪造版本」。

### 构造 SSLContext：强制 TLS 1.3

`configure_tls13_only` (`cert_util.py:664-690`) 显式设置 `minimum_version = maximum_version = TLSv1_3`，并选择 `TLS_AES_*_GCM_*` 系列密码套件。如果运行时环境不支持 `set_ciphersuites`，退化为「仅版本限制 + 默认套件」——这是一种优雅降级。

`construct_cert_context` (`cert_util.py:693-756`) 是「校验通过才返回 SSLContext」的统一入口：先做 CA、CRL、证书私钥三段严格校验，全部通过才创建 SSLContext 并 `load_verify_locations / load_cert_chain`。

## 四、SecurityUtils：纵深防御

`motor/common/http/security_utils.py` 不参与网络传输，而是给上层 HTTP 框架提供「敏感信息过滤」和「审计日志」工具：

- `filter_sensitive_headers` (`security_utils.py:27-42`)：黑名单 `authorization / cookie / x-api-key / proxy-authorization` 等，避免审计日志泄露凭据。
- `filter_sensitive_body` (`security_utils.py:45-69`)：递归过滤 body 中的 `password / token / api_key / secret / credit_card / ssn` 等敏感字段，深度限制为 3 层以避免在大结构上爆栈。
- `sanitize_error_message` (`security_utils.py:72-93`)：用正则把文件路径、堆栈位置、Traceback 等内部细节脱敏，并截断到 200 字符。
- `validate_and_sanitize_path` (`security_utils.py:128-160`)：URL 路径检查覆盖路径穿越 `..`、URL 编码 `%2e%2e / %2f / %5c`、Windows 非法字符 `<>:"|*?`、连续斜杠、超过 2048 字符路径——典型的 SSRF 与 LFI 输入验证。
- `validate_file_security` (`security_utils.py:163-177`)：检查配置文件是否为符号链接（symlink 攻击防护）以及权限位是否过宽。

`log_audit_event` (`security_utils.py:96-125`) 把请求时间、来源 IP、用户、事件、结果拼成结构化日志，便于对接 SOC/CCAE 平台做合规审计。

## 五、模块间关系与本目录其他文档

- **与 EtcdClient**：EtcdClient 自身的 gRPC 通道（`etcd_client.py:80-82`）也走 `ssl_channel_credentials`，但走的是 grpc 内置的根证书验证，与 HTTP 客户端的 CertUtil 各自独立——因为 gRPC 不允许直接替换 SSLContext。
- **与 Config 系统**：`tls_config` 来自 `motor/config/tls_config.py`，CertUtil 的入口参数 `tls_config: TLSConfig` 与 `controller.py:136-139` / `coordinator.py:357-359` 四个 TLS 子配置一一对应。
- **与 Alarm/Logger**：当 CertUtil 校验失败时会调用 `logger.error`，严重错误会通过 `AlarmStore`（见 T6_06）上报到北向平台。