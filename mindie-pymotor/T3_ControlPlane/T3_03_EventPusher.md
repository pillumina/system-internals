# T3_03_EventPusher

`EventPusher`（`motor/controller/core/event_pusher.py:30`）是 Controller 向 Coordinator 单向推送实例状态变更事件的模块。它实现 `Observer` 接口（详见 `T3_04_Observer模式.md`），订阅 `InstanceManager` 的状态变更，把事件异步推到 Coordinator 的 `POST /instances/refresh`（详见 `T2_01_Coordinator入口.md`）。本文梳理事件类型、推送时机、Coordinator 重连后的全量重同步机制。

## 事件类型

`EventType`（`motor/common/resources/http_msg_spec.py`）枚举了四种事件：

| 事件 | 触发条件 | 实例数 |
|------|---------|--------|
| `ADD` | ObserverEvent.INSTANCE_READY（实例进入 ACTIVE） | 1 |
| `DEL` | ObserverEvent.INSTANCE_SEPERATED 或 INSTANCE_REMOVED | 1 |
| `PAUSE` | ObserverEvent.INSTANCE_PAUSED | 1 |
| `RESUME` | ObserverEvent.INSTANCE_RESUMED | 1 |
| `SET` | Controller 重启恢复后 或 Coordinator 心跳丢失恢复 | 当前所有实例 |

`SET` 是"全量同步"事件，用于在 Controller 重启或 Coordinator 重连后让 Coordinator 端重建完整的 instance 视图。

## 事件订阅：`update`

`EventPusher.update(instance, event)`（`event_pusher.py:110-149`）是 Observer 通知入口：

```python
def update(self, instance: ReadOnlyInstance, event: ObserverEvent) -> None:
    if event == ObserverEvent.INSTANCE_READY:
        with self.lock:
            self.instances[instance.job_name] = instance
        event = Event(EventType.ADD, instance.to_instance())
    elif event == ObserverEvent.INSTANCE_SEPERATED:
        with self.lock:
            if instance.job_name in self.instances:
                del self.instances[instance.job_name]
            else:
                return                                          # INITIAL 阶段的 ABNORMAL 不投递
        event = Event(EventType.DEL, instance.to_instance())
    elif event == ObserverEvent.INSTANCE_PAUSED:
        with self.lock:
            if instance.job_name in self.instances:
                del self.instances[instance.job_name]
            else:
                return
        event = Event(EventType.PAUSE, instance.to_instance())
    elif event == ObserverEvent.INSTANCE_RESUMED:
        with self.lock:
            self.instances[instance.job_name] = instance
        event = Event(EventType.RESUME, instance.to_instance())
    elif event == ObserverEvent.INSTANCE_REMOVED:
        event = Event(EventType.DEL, instance.to_instance())
    else:
        return                                                  # INSTANCE_INITIAL 等不投递
    self.event_queue.put(event)
```

`self.instances` 是 EventPusher 维护的"已上报 ACTIVE 实例"快照：

- `READY`/`RESUMED` 加入；
- `SEPERATED`/`PAUSED` 删除；
- `REMOVED` 直接投递 DEL（DELETED 实例可能本来就没在快照里，所以不删）；
- `INITIAL` 事件被丢弃——InstanceManager 主动 `notify(ins, INSTANCE_INITIAL)` 仅在 `add_instance` 时发生，EventPusher 不应因此误推一个 ADD；READY 由后续心跳触发。

注意 `to_instance()` 把 ReadOnlyInstance 深拷贝回可写 Instance，确保 HTTP 投递过程中数据不会被 InstanceManager 并发修改。

## 事件投递

`push_event(event_type)`（`event_pusher.py:151-154`）允许外部直接 push 一个无 instance 的事件（典型用途：Controller 重启后推 `SET`）。事件队列是 `queue.Queue`，跨线程安全。

`_event_consumer`（`event_pusher.py:156-199`）是消费循环：

```python
def _event_consumer(self) -> None:
    while not self.stop_event.is_set():
        try:
            event = self.event_queue.get(timeout=1.0)
        except queue.Empty:
            continue
        event_type = event.event_type
        if event_type == EventType.ADD:
            event_msg = InsEventMsg(event=event_type, instances=[event.instance])
        elif event_type in (EventType.DEL, EventType.PAUSE, EventType.RESUME):
            event_msg = InsEventMsg(event=event_type, instances=[event.instance])
        elif event_type == EventType.SET:
            with self.lock:
                instances = list(self.instances.values())
                has_prefill = any(inst.role == "prefill" for inst in instances)
                if has_prefill:
                    event_msg = InsEventMsg(
                        event=event_type,
                        instances=[instance.to_instance() for instance in instances],
                    )
                else:
                    logger.debug("SET event skipped: requires at least one prefill instance, ...")
                    event_msg = None
        else:
            logger.error("Unknown event type: %s", event_type)
            continue

        if event_msg is not None:
            try:
                CoordinatorApiClient.send_instance_refresh(event_msg)
            except Exception as e:
                logger.error("Failed to send instance refresh event, error: %s", e)
```

关键点：

- `get(timeout=1.0)` 让消费线程在空队列时也能每 1s 检查 `stop_event`，避免 `join()` 永久阻塞；
- `SET` 事件要求至少有一个 prefill 实例——这是早期约定，防止 Controller 在 P 实例全空时给 Coordinator 推一个空集群视图导致 routing 立刻报 503；
- `send_instance_refresh` 失败仅记 error 不会重试，依靠 Coordinator 端 `_re_register_loop` 周期任务兜底（详见下文）。

## Coordinator 重连与 SET

`_coordinator_heartbeat_detector`（`event_pusher.py:201-263`）按 `coordinator_heartbeat_interval` 周期 ping Coordinator：

```python
def _coordinator_heartbeat_detector(self) -> None:
    hb_loss_cnt = 0
    while not self.stop_event.is_set():
        try:
            params = {"status": "normal"}
            response = CoordinatorApiClient.query_status(params)
            if not self.is_first_heartbeat_success:
                self.is_first_heartbeat_success = True
                logger.info("Coordinator heartbeat established successfully.")
            if response is None or response.get("ready") is None or not response.get("ready"):
                self.is_coordinator_reset = True
            if self.is_coordinator_reset:
                event = Event(EventType.SET, None)
                self.event_queue.put(event)
                self.is_coordinator_reset = False
                hb_loss_cnt = 0
        except Exception as e:
            if self.is_first_heartbeat_success:
                hb_loss_cnt += 1
                if hb_loss_cnt >= 2:
                    self.is_coordinator_reset = True
                    logger.warning("Coordinator heartbeat lost. Possible restart detected.")
                    hb_loss_cnt = 0
        with self.config_lock:
            heartbeat_interval = self.coordinator_heartbeat_interval
        with self.work_condition:
            self.work_condition.wait(timeout=heartbeat_interval)
```

判定逻辑：

- Coordinator 返回非 ready（not master / heartbeat stale） → `is_coordinator_reset = True`，下一次循环推 SET；
- 连续 2 次心跳异常（且至少成功过一次） → `is_coordinator_reset = True`；
- 推 SET 后重置 `is_coordinator_reset = False` 与 `hb_loss_cnt = 0`；
- `work_condition.wait(timeout)` 兼顾 stop_event 唤醒与周期轮询。

## Coordinator 端接收路径

`ManagementServer._handle_refresh_instances`（`motor/coordinator/api_server/management_server.py:361-425`）：

1. 校验 body 大小 ≤ 10MB、非空、合法 JSON；
2. 用 `InsEventMsg` Pydantic 模型反序列化；
3. 同时调 `scheduler_connection.refresh_instances(event, instances)`（让 Scheduler 进程的视图同步）与 `instance_manager.refresh_instances(event, instances)`（让 Mgmt 进程的视图同步）；
4. 返回成功响应，附带时间戳、event_type、instance_count。

Mgmt 进程侧的 `_re_register_loop`（`management_server.py:186-210`）每 `re_register_interval_sec` 调 `ConductorApiClient.re_register_kv_instances(instances)` 把 KVA-eligible 实例（`ROLE_P`/`ROLE_U`）补注册到 Conductor，弥补 EventPusher 推送遗漏导致的 KV 注册缺失。

## 启动与停止

`start()`（`event_pusher.py:59-73`）启动两个 daemon 线程（`event_consumer`、`heartbeat_detector`）。`stop()`（`event_pusher.py:75-94`）：

1. `stop_event.set()`；
2. `work_condition.notify_all()` 唤醒可能阻塞的 detector；
3. `join` 两个线程；
4. 关闭 `heart_client`（`CoordinatorApiClient` 持有的 HTTP 连接）。

`is_alive()` 同时检查两个线程是否 alive，用于 `controller/main.py:198` 的 `stop_all_modules` 跳过未启动的模块。

## 配置热更新

`update_config(config)`（`event_pusher.py:105-108`）只更新 `coordinator_heartbeat_interval`，不停线程。下一次 `_coordinator_heartbeat_detector` 循环会用新间隔。

## 跨文档引用

- 状态变更触发源（InstanceManager）见 `T3_02_InstanceManager.md`。
- 生命周期具体转换见 `T3_02_1_实例生命周期.md`。
- Observer 接口与通知链见 `T3_04_Observer模式.md`。
- Coordinator `POST /instances/refresh` 入口与 Schema 校验见 `T2_01_Coordinator入口.md`。
- Conductor 注册路径见 `motor/coordinator/api_client/conductor_api_client.py`。
