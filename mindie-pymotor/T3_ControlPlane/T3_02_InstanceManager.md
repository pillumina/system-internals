# T3_02_InstanceManager

`InstanceManager`（`motor/controller/core/instance_manager.py:47`）是 Controller 中所有 PD 实例状态的事实来源。它是 `ThreadSafeSingleton`（`controller/main.py:153` 通过 `InstanceManager(config)` 实例化，但全局只有一份），承担实例增删、心跳处理、状态机转移、强分离控制、ETCD 持久化、Observer 通知等职责。本文基于 `instance_manager.py` 的源码梳理其内部结构与状态机驱动逻辑。

## 内部数据结构

```python
# instance_manager.py:69-101
self.instances: dict[int, Instance] = {}
self.observers: list[Observer] = []
self.forced_separated_instances: set[int] = set()    # 强分离集合，心跳无法自动拉回
self.stop_event = threading.Event()
self.ins_lock = threading.Lock()
self.config_lock = threading.RLock()
self.etcd_client = EtcdClient(etcd_config=..., tls_config=...)
self._data_version = 0
self._version_lock = threading.Lock()
```

- `instances` 是核心 `instance_id -> Instance` 字典，所有读操作都通过 `with self.ins_lock` 保护；
- `forced_separated_instances` 防止"被运维强制隔离的实例"被后续心跳自动恢复到 ACTIVE——这是 A5 虚推与故障隔离的关键；
- `etcd_client` 在 `enable_etcd_persistence=True` 时把状态写入 `/controller/instance_manager`，用于 Controller 重启恢复；
- `_data_version` 单调递增，与 PersistentState 一起写入 ETCD，保证 Controller 重启后不会读到旧数据。

## 状态与转换表

`InstanceManager` 定义五种实例状态（`InsStatus`）和五种事件（`InsConditionEvent`）：

```python
# instance_manager.py:94-126
self.states = {
    InsStatus.INITIAL:  self._handle_initial,
    InsStatus.ACTIVE:   self._handle_active,
    InsStatus.INACTIVE: self._handle_inactive,
    InsStatus.PAUSED:   self._handle_paused,
    InsStatus.DELETED:  self._handle_deleted,
}
self.transitions = {
    (InsStatus.INITIAL,   InsConditionEvent.INSTANCE_INIT):                InsStatus.INITIAL,
    (InsStatus.INITIAL,   InsConditionEvent.INSTANCE_NORMAL):             InsStatus.ACTIVE,
    (InsStatus.INITIAL,   InsConditionEvent.INSTANCE_ABNORMAL):           InsStatus.INACTIVE,
    (InsStatus.INITIAL,   InsConditionEvent.INSTANCE_HEARTBEAT_TIMEOUT):  InsStatus.DELETED,
    (InsStatus.ACTIVE,    InsConditionEvent.INSTANCE_NORMAL):             InsStatus.ACTIVE,
    (InsStatus.ACTIVE,    InsConditionEvent.INSTANCE_HEARTBEAT_TIMEOUT):  InsStatus.INACTIVE,
    (InsStatus.ACTIVE,    InsConditionEvent.INSTANCE_ABNORMAL):           InsStatus.INACTIVE,
    (InsStatus.INACTIVE,  InsConditionEvent.INSTANCE_ABNORMAL):           InsStatus.INACTIVE,
    (InsStatus.INACTIVE,  InsConditionEvent.INSTANCE_NORMAL):             InsStatus.ACTIVE,
    (InsStatus.INACTIVE,  InsConditionEvent.INSTANCE_INIT):               InsStatus.INITIAL,
    (InsStatus.INACTIVE,  InsConditionEvent.INSTANCE_HEARTBEAT_TIMEOUT):  InsStatus.DELETED,
    (InsStatus.ACTIVE,    InsConditionEvent.INSTANCE_PAUSED):             InsStatus.PAUSED,
    (InsStatus.INACTIVE,  InsConditionEvent.INSTANCE_PAUSED):             InsStatus.PAUSED,
    (InsStatus.INITIAL,   InsConditionEvent.INSTANCE_PAUSED):             InsStatus.PAUSED,
    (InsStatus.PAUSED,    InsConditionEvent.INSTANCE_PAUSED):             InsStatus.PAUSED,
    (InsStatus.PAUSED,    InsConditionEvent.INSTANCE_RESUMED):            InsStatus.ACTIVE,
    (InsStatus.PAUSED,    InsConditionEvent.INSTANCE_NORMAL):             InsStatus.ACTIVE,
    (InsStatus.PAUSED,    InsConditionEvent.INSTANCE_HEARTBEAT_TIMEOUT):  InsStatus.DELETED,
    (InsStatus.PAUSED,    InsConditionEvent.INSTANCE_ABNORMAL):           InsStatus.INACTIVE,
}
```

转换表覆盖了 INITIAL/ACTIVE/INACTIVE/PAUSED/DELETED 之间的所有合理迁移。`INSTANCE_HEARTBEAT_TIMEOUT` 最终会走到 `DELETED`，对应"心跳长时间没续，实例彻底退出集群"。

## 心跳处理

`handle_heartbeat(heartbeat_msg)`（`instance_manager.py:563-595`）：

```python
def handle_heartbeat(self, heartbeat_msg: HeartbeatMsg) -> tuple[bool, str]:
    ins_id = heartbeat_msg.ins_id
    pod_ip = heartbeat_msg.ip
    timestamp = time.time()
    with self.ins_lock:
        instance = self.instances.get(ins_id, None)
    if instance is None:
        raise HTTPException(HEARTBEAT_HANDLER_RE_REGISTER)
    if instance.update_heartbeat(pod_ip, timestamp, heartbeat_msg.status):
        ...
    if self._handle_state_transition(instance):
        return True, HEARTBEAT_HANDLER_SUCCESS
    else:
        raise HTTPException(HEARTBEAT_HANDLER_ERROR)
```

`update_heartbeat` 把当前时间戳与端点状态写到 `Instance.endpoints[pod_ip][endpoint_id]` 上，再由 `_handle_state_transition` 根据当前所有端点的状态推断事件并执行状态机转移。

## 状态机驱动

`_handle_state_transition`（`instance_manager.py:777-857`）是核心决策逻辑：

```python
def _handle_state_transition(self, instance, event_override=None) -> bool:
    from_state = instance.status
    if event_override is not None:
        event = event_override
        to_state = self.transitions.get((from_state, event), None)
    elif instance.is_all_endpoints_paused():
        event = InsConditionEvent.INSTANCE_PAUSED
        ...
    elif instance.is_all_endpoints_ready():
        event = InsConditionEvent.INSTANCE_NORMAL
        ...
    elif instance.is_have_one_endpoint_abnormal():
        event = InsConditionEvent.INSTANCE_ABNORMAL
        ...
    elif instance.is_any_endpoint_paused():
        # 混合 PAUSED + NORMAL/INITIAL（无 ABNORMAL）：视为 PAUSED 事件
        # 防止 INACTIVE → INITIAL 时 PreStop PAUSED 覆盖 ABNORMAL
        event = InsConditionEvent.INSTANCE_PAUSED
        ...
    else:
        event = InsConditionEvent.INSTANCE_INIT
        ...
    ...
    # 强分离保护：to_state == ACTIVE 时阻止自动恢复
    if instance.id in self.forced_separated_instances and to_state == InsStatus.ACTIVE:
        return True                              # 返回 success 但跳过状态转移
    state_handler = self.states.get(to_state, None)
    if state_handler:
        state_handler(from_state, event, instance)
        if from_state != instance.status and enable_persistence:
            self.persist_data()                  # 状态真正变了才持久化
        # 转移到 DELETED 时自动从强分离集合移除
        if to_state == InsStatus.DELETED and instance.id in self.forced_separated_instances:
            self.forced_separated_instances.discard(instance.id)
```

要点：

- 事件推断顺序：`PAUSED > READY > ABNORMAL > MIXED-PAUSED > INIT`；
- 强分离保护：被运维显式 `separate_instance` 的实例即使收到正常心跳也不会回到 ACTIVE；
- 持久化：仅当状态真正变化（`from_state != instance.status`）且 ETCD 启用时才写盘；
- `transition` 后由 `self.states[to_state]` 状态处理器做副作用：触发 Observer 通知、上报告警。

## 强分离与恢复

`separate_instance(instance_id)`（`instance_manager.py:426-472`）与 `recover_instance(instance_id)`（`instance_manager.py:486-524`）是一对：

- `separate_instance`：把实例加入 `forced_separated_instances`，若当前是 ACTIVE 则降级为 INACTIVE 并 notify；
- `recover_instance`：从 `forced_separated_instances` 移除，让实例通过正常心跳回到 ACTIVE；若同 `job_name` 已有更新的实例，则只移除旧实例的强分离标记（避免误恢复）。

## ETCD 持久化与恢复

`persist_data`（`instance_manager.py:193-233`）：

1. 拿 `ins_lock`，生成下一个 `_data_version`；
2. 把 `instances` 全量 dump 为 `{ins_id: instance.model_dump()}`；
3. 包成 `PersistentState(data, version, timestamp, checksum)`，checksum 用 `calculate_checksum`；
4. 写入 `/controller/instance_manager`；
5. **释放 `ins_lock` 后才做 ETCD I/O**，避免阻塞心跳处理。

`restore_data`（`instance_manager.py:235-299`）反向：

1. 从 ETCD 读 `PersistentState`，校验 checksum；
2. 更新 `_data_version = max(self._data_version, persistent_state.version)`；
3. 重建 `Instance` 对象并按需刷新心跳；
4. 向 `EventPusher` 推 `EventType.SET` 让 Coordinator 重新同步所有实例状态。

ETCD 关闭时（`enable_persistence=False`）也调用 `_maybe_refresh_heartbeat` 让恢复出来的 ACTIVE 实例不会立即被超时回收。

## 后台管理线程

`start`/`stop`/`_instances_management_loop`（`instance_manager.py:132-164, 599-617`）：每 `instance_manager_check_interval` 秒扫描所有非 DELETED 实例，若 `is_all_endpoints_alive()` 为 False 则用 `INSTANCE_HEARTBEAT_TIMEOUT` 事件触发 `_handle_state_transition`。

## 公共查询接口

- `get_active_instances`/`get_initial_instances`/`get_inactive_instances`：按状态过滤；
- `get_instances(statuses=None)`：None 时返回 INITIAL+ACTIVE+INACTIVE；
- `get_instance_by_podip`/`get_instance_by_job_name`/`has_active_instance_by_job_name`：按 IP/job_name 查找；
- `get_instances_by_role(role)`：按 PD role 过滤。

这些都是 Controller 其他模块（FaultManager、Observability）做决策的数据源。

## 跨文档引用

- 实例状态机的具体转换与触发条件见 `T3_02_1_实例生命周期.md`。
- PD 角色动态分配（`InstanceAssembler`）见 `T3_02_2_实例身份分配.md`。
- Observer 通知与 EventPusher 的衔接见 `T3_03_EventPusher.md`、`T3_04_Observer模式.md`。
- 与 etcd 的持久化协议见 `motor/common/etcd/persistent_state.py`。
