This folder contains the initial implementation of a so-called Byzantine testing extension for the Apollo framework. The idea is to provide a series of extension points within the regular replica logic to allow the injection of certain Byzantine behavior.

From the implementation point of view:

```
- main.cpp
- ByzantineReplicaImp.hpp
- ByzantineReplicaImp.cpp
```
Those contain the main interfaces and classes to start a Byzantine replica with the proper settings in the Apollo tests context and to extend the default replica logic in order to add different Byzantine behavior.

```
- setup.hpp
- setup.cpp
- internalCommandsHandler.hpp
- internalCommandsHandler.cpp
```
Those contain helper interfaces and clases for replica initialization purposes in the Apollo tests context. They are actually pretty similar to the ones within the `../TesterReplica` folder, and with a bit of refactoring they could be shared and reused.

At tests level, the `test_skvbc_byzantine.py` file contains a simple test that makes use of a Byzantine replica instance.