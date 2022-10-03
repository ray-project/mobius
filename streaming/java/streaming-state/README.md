## Module Functionality & Relationship

* api: The api of the _streaming-state_ that is used to read/write states. Including the following sub-modules:
  * state: Offering states of various type, including:
    * `ListState`: states in list format
    * `MapState`: states in key-value format
    * `ValueState`: A special MapState whose key is a default hidden value
  * desc：Contains various _Descriptors_ to describe & build corresponding State
  * StateType: Only [Map, Value] now, the ListState is not supported yet!
  
* store: Persistent `State` in the actual file system(including Memory). 
Each kind of backend file system should implement the persistent method for each State type separately, under their own submodule.
  * backend: the core of a store that implements all critical functions a state store should equip, such as rollback, snapshot, etc. 

* manager: `StateManager` that builds the corresponding _State_ by the given _Descriptor_, usually maintained directly by upstream users. 

## UML
![XHUfUI.png](https://s1.ax1x.com/2022/06/16/XHUfUI.png)