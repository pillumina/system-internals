# T3_04_Observer模式

Controller 中多个子模块都需要响应实例状态变更：EventPusher 要推送事件给 Coordinator，FaultManager 要触发恢复决策，Observability 要上报告警。Observer 模式让 `InstanceManager` 与这些模块解耦——它只需在状态变更时通知所有订阅者，订阅者各自决定如何响应。本文梳理接口定义、事件枚举、`attach`/`notify` 协作，以及当前的两个核心订阅者。

## 接口定义

Observer 接口在 `motor/controller/core/observer.py`：

```python
class ObserverEvent(Enum):
    INSTANCE_INITIAL   = 0   # 实例被 add_instance 进来
    INSTANCE_READY     = 1   # INITIAL/INACTIVE → ACTIVE
    INSTANCE_SEPERATED = 2   # ACTIVE → INACTIVE（强分离/ABNORMAL）
    INSTANCE_REMOVED   = 3   # 任意状态 → DELETED
    INSTANCE_PAUSED    = 4   # 任意状态 → PAUSED
    INSTANCE_RESUMED   = 5   # PAUSED → ACTIVE

class Observer(ABC):
    @abstractmethod
    def update(self, instance: ReadOnlyInstance, event: ObserverEvent) -> None:
        pass
```

注意 `INSTANCE_SEPERATED` 是历史拼写错误（少了一个 `A`），全系统已统一沿用，命名修改会带来广泛的 import 路径变更，得不偿失。

## InstanceManager 的注册与通知

`InstanceManager` 维护 `self.observers: list[Observer] = []`（`instance_manager.py:70`）。`attach(observer)`（`instance_manager.py:182-185`）做去重追加：

```python
def attach(self, observer: Observer) -> None:
    if observer not in self.observers:
        self.observers.append(observer)
```

`notify(instance, event)`（`instance_manager.py:188-191`）广播：

```python
def notify(self, instance: Instance, event: ObserverEvent) -> None:
    readonly_instance = ReadOnlyInstance(instance)
    for observer in self.observers:
        observer.update(readonly_instance, event)
```

Observer 收到的是 `ReadOnlyInstance` 而不是 `Instance`——这是非常重要的设计约束：

1. Observer 内部对实例的任何写操作必须通过 `InstanceManager` 的 API（`separate_instance`/`recover_instance`/...），不能绕过；
2. Observer 在另一线程（典型：EventPusher 的 `event_consumer`）处理事件时，并发修改 `Instance` 字段会破坏 InstanceManager 的状态机；
3. 若 Observer 需要修改实例，调用方负责把 ReadOnlyInstance 转回 Instance（`instance.to_instance()`）并按 API 提交。

## 何时触发 notify

`InstanceManager` 自身不直接调 `notify`，而是在每个状态处理器内部：

- `_handle_initial`（`instance_manager.py:619-627`）：`from_state == INACTIVE && event == INSTANCE_INIT` 时 `update_instance_status(INITIAL)`——但**不** notify（回退到 INITIAL 不视为外部可见事件）；
- `_handle_active`（`instance_manager.py:629-640`）：`RESUMED` → `notify(INSTANCE_RESUMED)`；`NORMAL` → `notify(INSTANCE_READY)`；
- `_handle_inactive`（`instance_manager.py:642-674`）：`ABNORMAL` 或心跳超时后置 INACTIVE → `notify(INSTANCE_SEPERATED)`；
- `_handle_paused`（`instance_manager.py:676-681`）：进入 PAUSED → `notify(INSTANCE_PAUSED)`；
- `_handle_deleted`（`instance_manager.py:765-775`）：进入 DELETED → `notify(INSTANCE_REMOVED)` + `del_instance`；
- `add_instance`（`instance_manager.py:388`）：实例新增 → `notify(INSTANCE_INITIAL)`；
- `separate_instance`（`instance_manager.py:444`）：主动分离（ACTIVE→INACTIVE）→ `notify(INSTANCE_SEPERATED)`。

也就是说，**任何外部可见的状态变化都会触发对应 ObserverEvent**，但**回退性**变化（如 INACTIVE→INITIAL）不触发，避免噪音。

## Observer 订阅者

Controller 中实现 Observer 的核心模块：

### EventPusher

`update`（`event_pusher.py:110-149`）按事件类型把实例装入 `Event` 并入队（详见 `T3_03_EventPusher.md`）。`READY`/`RESUMED` 维护"已推送 ACTIVE 实例"快照；`SEPERATED`/`PAUSED` 从快照删除；`REMOVED` 不维护快照直接投递 DEL。

`EventPusher.push_event(EventType.SET)`（`event_pusher.py:151-154`）允许外部主动触发全量重同步，例如 `InstanceManager.restore_data` 在 ETCD 恢复后会找到第一个 EventPusher 类型的 Observer 并直接 push SET：

```python
# instance_manager.py:286-289
for observer in self.observers:
    if isinstance(observer, EventPusher):
        observer.push_event(EventType.SET)
        break
```

### FaultManager

`update` 把故障实例纳入恢复决策池——`SEPERATED` 事件触发"是否需要 ScaleP2D 扩容"的评估，`REMOVED` 触发"是否需要替换实例"的决策。FaultManager 仅在 `fault_tolerance_config.enable_fault_tolerance=True` 时被注册（`main.py:166-170` 的 observers_list 控制）。

### Observability（潜在订阅者）

`Observability` 通过 `Observability().add_alarm(alarm)` 上报告警，但它本身**不直接订阅** Observer——告警由状态处理器显式调 `_report_inst_alarm`/`_report_coordinator_alarm`（`instance_manager.py:683-711`）触发。把告警上报与 Observer 解耦是有意的：告警语义比 ObserverEvent 丰富得多（cleared/severity/timestamp），直接复用 Observer 反而要扩接口。

## 启动顺序

`main.py:161-170` 的 `init_all_modules` 完成后才挂载 Observer：

```python
for module_name, module in modules.items():
    if module_name in observers_list:
        logger.info("Attaching %s to instance manager", module_name)
        instance_manager.attach(module)
```

observers_list 仅含 `{"EventPusher", "FaultManager"}`。这保证了 Observer 在 InstanceManager 启动后才能收到事件，避免"启动期的事件被丢弃"。

## ReadOnlyInstance 与 to_instance()

`ReadOnlyInstance` 是 `Instance` 的只读包装（`motor/common/resources/instance.py`）。`to_instance()`（EventPusher 在 update 中调）反向把 ReadOnlyInstance 深拷贝回可写 Instance，供 HTTP 投递使用。这一拷贝确保 Observer 处理事件的整个生命周期里，InstanceManager 即使修改了原始实例也不会影响已发出去的事件 payload。

## 跨文档引用

- InstanceManager 的状态机与转换表见 `T3_02_InstanceManager.md`。
- 状态机转移规则与触发条件见 `T3_02_1_实例生命周期.md`。
- EventPusher 如何把 Observer 事件翻译成 HTTP 推送见 `T3_03_EventPusher.md`。
- Controller 启动装配 Observer 的顺序见 `T3_01_Controller角色定位.md`。
