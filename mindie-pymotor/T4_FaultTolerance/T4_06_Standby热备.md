# T4_06 Standby 热备

## 目标

Controller 与 Coordinator 都以多副本方式部署（典型为 2 副本），避免单点故障。`StandbyManager` 是主备管理器，统一抽象"主备选举 + 心跳保活 + 角色回调"三件事，让上层模块只需关心 `on_become_master` / `on_become_standby` 两个回调，无需直接操作 ETCD 锁。

实现位于 `motor/common/standby/standby_manager.py`，复用 `ThreadSafeSingleton` 保证全局唯一。

## 核心角色与状态

```python
class StandbyRole(Enum):
    STANDBY = "standby"
    MASTER = "master"
```

每个进程的 `StandbyManager` 实例持有 `current_role`（默认 `STANDBY`）与 `role_lock`。`is_master()` 在锁内读取，是上层模块最常用的判断接口。

## ETCD 锁模型

`StandbyManager` 通过 ETCD 的 lease + lock 原语实现主备选举（`standby_manager.py:211-246`）：

- `acquire_lock(lock_key, ttl=lock_ttl)`：获取 lease 绑定的分布式锁，TTL 由配置 `master_lock_ttl` 决定（默认较长，保证主备切换时锁不会因网络抖动过早释放）。
- `renew_lease(lock_key)`：Master 周期续约，避免因 GC 停顿导致 lease 过期。
- `release_lock(lock_key)`：停止时主动释放。

`max_lock_failures` 与 `lock_retry_interval`（从 `StandbyConfig` 读取）控制获取失败时的退避：连续失败超过阈值则放弃本次 master 抢占。

## 主备循环

`_master_standby_loop`（`standby_manager.py:159-190`）以 `master_standby_check_interval` 周期运行：

1. **作为 Master**：续约 lease。失败则降级为 Standby 并触发 `on_become_standby()`。
2. **作为 Standby**：尝试 `acquire_lock`。成功后切到 Master，触发 `on_become_master(should_report_event)`，并把 ETCD `/controller/should_report_event` 置为 True，避免告警在主备切换中丢失。
3. **停止时**：若是 Master 则释放锁，把角色 shm 复位为 Standby。

`start()`（`standby_manager.py:92-115`）注入两个回调；`stop()`（`standby_manager.py:117-140`）通过 `stop_event` 中断循环并 join 线程。

## 上层接入方式

Controller 与 Coordinator 在启动时各自调用 `StandbyManager.start(on_become_master, on_become_standby, report_event_key)`：

- Controller：`report_event_key = "/controller/should_report_event"`（`standby_manager.py:26`）。
- Coordinator：`report_event_key = "/coordinator/should_report_event"`（`standby_manager.py:27`）。

`on_become_master(should_report_event)` 中的 `should_report_event` 由 ETCD 传递，用于决定新 Master 是否需要补发切换期间的告警事件——这是主备切换期间数据不丢失的关键设计。

## 与 ETCD 客户端的协作

- 锁与 lease 由 `EtcdClient` 提供（`standby_manager.py:53`），与 FaultManager 的 ETCD 持久化共用同一客户端类，但使用不同的 key 前缀（`master_lock_key` 与 FaultManager 的节点/实例存储），互不干扰。
- `stop()` 关闭 ETCD 客户端，保证资源释放（`standby_manager.py:134-136`）。

## 并发与边界条件

- **续约失败**：Master 续约失败不立即退出，而是降级为 Standby 并触发回调，让上层模块有机会停止后台任务。
- **重复启动**：`start()` 检查 `is_running`，避免重复拉起后台线程（`standby_manager.py:99-101`）。
- **Singleton 复用**：`is_initialized` 与 `reset_instance`（`standby_manager.py:79-87`）允许在测试或热重启场景下重置单例。
- **角色翻转期间的回调顺序**：`_master_standby_loop` 严格先 `set_role`，再触发回调，回调中可以安全读取 `is_master()` 而无需自旋等待。

## 文档交叉引用

- ETCD 客户端细节：T6 基础设施章节。
- 主备切换期间告警补发与 FaultManager 配合：T4_02_FaultManager.md、T8_DeepDives 中的告警通道。
- Controller / Coordinator 启动接入主备的入口：T3_ControlPlane 中的 Controller 启动序列。