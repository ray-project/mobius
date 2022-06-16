## 各模块功能 & 关系
* keystate：_streaming-state_ 的 api，其内容包括：
  * state：提供各类型 State 的读写接口，类型包括
    * `KeyMapState`
    * `KeyValueState`
    * `ListState`
    * `ValueState`
  * desc：包含各类 _Descriptor_ 用于描述不同类型 State Schema 的
  * `StateManager`：用于根据所提供的 _Descriptor_ 来构建相应的 _State_。通常由上层用户直接维护。
    * `stateMap`：维护所有 State，格式 < stateName, State >
* store：存储 _State_ ，每一种 _State_ 对应一个 _Store_ 接口。每个 _Store_ 接口又可由不同类型的存储方式实现（目前开源版本仅支持 MemoryStore）
  * memory：Memory 类型的存储方式，支持存储的 `State` 格式包括：[KeyValue, KeyMap]
  * `StateStoreManager`：为 _State_ 提供用于存储相关的功能接口
  * `StateStoreFactory`：`StateStoreManager` 的工厂类
* backend：负责实际实现 _State_ 在不同 backend 中的读写，每种 _StateStore_ 均需要一个相应的 backend。
  * `StateBackendType`：支持的 backend 类型，目前包括：[Memory]
* strategy：TODO
* config：（略）
* serialization：（略）

## UML
![XHUfUI.png](https://s1.ax1x.com/2022/06/16/XHUfUI.png)