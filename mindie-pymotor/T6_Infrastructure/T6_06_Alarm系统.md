# T6_06 Alarm 系统：AlarmStore 与北向平台对接

Motor 把告警（Alarm）作为独立子系统暴露——`motor/controller/observability/alarm/alarm_store.py` 的 `AlarmStore` 是进程内单例，负责接收故障事件、本地去重、并在北向平台（CCAE）拉取时把当前活跃告警打包返回。本节梳理它的去重策略、单例模式、与北向接口协议。

## 一、AlarmStore 的角色与位置

`alarm_store.py:24-77` 的 `AlarmStore` 继承自 `ThreadSafeSingleton`（`motor/common/utils/singleton.py`）。这意味着一个进程内只有一个 AlarmStore 实例，所有告警路径（包括 Controller、Coordinator 内部模块、后台故障检测线程）都往这一个对象里写。

构造 (`alarm_store.py:27-34`) 初始化两个数据结构：

```python
self._alarms: dict[str, list] = {os.getenv("NORTH_PLATFORM", "").strip(): []}
self._recoverable_alarms: dict[str, Record] = {}
```

- `_alarms`：按「北向平台 source_id」分组的活跃告警列表。key 默认取环境变量 `NORTH_PLATFORM`，方便多集群/多租户场景下不同 source_id 收集不同告警；
- `_recoverable_alarms`：仅对可恢复告警维护的 `(alarm_id + instance_id) → Record` 索引，用于跟踪「上报了但还没恢复」的实例异常。

锁用 `self._dict_lock = threading.Lock()` (`alarm_store.py:32`) 保护两个数据结构——AlarmStore 接受任意线程调用，但写路径必须串行。

## 二、Record：告警数据结构

`motor/common/alarm/record.py` 定义 `Record`（未在本节源码范围内，但被 alarm_store 引用）。一个 Record 至少包含：

- `alarm_id`：告警唯一标识，引用 `INSTANCE_EXCEPTION_ALARM_ID` / `COORDINATOR_EXCEPTION_ALARM_ID` 等常量；
- `instance_id`：触发实例的 ID（仅实例级告警有）；
- `cleared`：枚举 `Cleared.YES / Cleared.NO`，标识「告警已清除 vs 仍存在」；
- `format()`：方法，把 Record 序列化成 dict 给北向接口。

## 三、add_alarm：两种处理路径

`add_alarm` (`alarm_store.py:36-53`) 是唯一写入入口。它根据 `alarm_id` 类型分派：

### 普通告警：无去重直接入队

```python
for value in self._alarms.values():
    value.append(record)
```

普通告警不维护去重状态——同一个 alarm_id 多次上报会形成列表，北向平台拉取时看到「同一个告警 N 次」可由北向侧做去重。这种「客户端简单、北向侧聚合」的设计让 AlarmStore 不需要复杂状态。

### 实例异常告警：去重 + 恢复跟踪

`alarm_id in (INSTANCE_EXCEPTION_ALARM_ID, COORDINATOR_EXCEPTION_ALARM_ID)` 走 `_handle_instance_exception_alarm` (`alarm_store.py:62-77`)。它的设计目标是「同一实例的同一种异常不要重复刷屏」：

```python
recovery_alarm_key = f"{alarm_id}_{instance_id}"

if record.cleared == Cleared.NO and key not in _recoverable_alarms:
    # 新告警：入队 + 记录为「待恢复」
    self._recoverable_alarms[recovery_alarm_key] = record
elif record.cleared == Cleared.YES and key in _recoverable_alarms:
    # 恢复事件：入队 + 从待恢复索引里删
    del self._recoverable_alarms[recovery_alarm_key]
```

具体语义：

| 上报状态 | 当前索引状态 | 行为 |
|---------|------------|------|
| `cleared=NO`（新故障） | 索引里没有 | 入活跃列表 + 加入可恢复索引 |
| `cleared=NO`（重复故障） | 索引里已有 | 不入活跃列表，不更新索引 |
| `cleared=YES`（恢复事件） | 索引里没有 | 不入活跃列表（这是异常：恢复事件比告警事件晚到） |
| `cleared=YES`（恢复事件） | 索引里有 | 入活跃列表（恢复事件需要被北向看到）+ 从索引删除 |

第三种情况「恢复事件比告警事件晚到」会被静默忽略——这是「最后写入者赢」倾向的实现，假设故障检测线程会持续重试心跳所以最终告警会上报到。

`logger.debug` 输出 (`alarm_store.py:64-69`) 在 debug 级别打印当前恢复索引的 key 列表和本次 record 的 cleared 状态，便于排查「为什么我的告警没出现」。

## 四、get_alarms：北向拉取接口

`get_alarms(source_id)` (`alarm_store.py:55-60`) 是北向平台（CCAE）拉取告警的入口：

```python
with self._dict_lock:
    result = [record.format() for record in self._alarms.get(source_id, [])]
    self._alarms[source_id] = []  # 拉取即清空
    return [result] if result else []
```

两个关键设计：

1. **拉取即清空**：北向平台调用 `get_alarms` 后，本地列表被立刻清空——下个周期只上报新产生的告警。这种「pull-based 去重」让客户端不需要维护 offset/cursor；
2. **空结果返回空列表**：北向如果收到 `[]` 表示本周期无新告警，与「格式错误」明确区分。

返回结构是 `list[list[dict]]`——外层只有一个元素（当前周期），内层是该周期的所有告警字典。这种结构允许北向接口在不破坏兼容性的情况下，扩展为「多个周期的批次」。

## 五、CCAE 集成模式

虽然 `alarm_store.py` 不直接包含 HTTP 请求代码，但从调用模式可以推断：

- Controller 内部有一个定时任务（参考 controller.py 启动序列）周期性调用 `AlarmStore.get_alarms("north-platform-id")`；
- 拿到结果后通过 HTTPS POST 到 CCAE 接口；
- 失败重试走 `ExceptionConfig.max_retry`（在 coordinator 侧也有同样语义）。

`_alarms` 字典的 key 设计为 `os.getenv("NORTH_PLATFORM", "")` 让同一份代码可以服务多个北向平台——多 key 时每个 source_id 独立维护活跃列表与拉取状态。这种「多 sink 分桶」设计在大规模部署（一个 Motor 集群向多个监控后端上报）很有用。

## 六、模块关系

- **与 Logger（T6_03）**：`logger.debug` / `logger.error` 输出告警流程状态；严重失败（如 add_alarm 抛异常被外层捕获）会被 ErrorLog 通过北向日志通道上报；
- **与 Fault Tolerance（T4）**：故障检测线程发现实例故障时，构造 `Record(alarm_id=INSTANCE_EXCEPTION_ALARM_ID, cleared=Cleared.NO, instance_id=...)` 调 `add_alarm`；
- **与 Config（T6_05）**：`ControllerConfig.fault_tolerance_config.scale_p2d_d_instance_reinit_wait_timeout` 等参数影响「告警触发后多久执行恢复动作」，是告警 → 行动的桥梁；
- **与 EtcdClient（T6_01）**：在某些多 Controller 部署里，告警状态会通过 etcd 共享（待恢复实例列表跨 Controller 同步），避免主备切换时告警丢失。