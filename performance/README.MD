# PerformanceManager class

This class is a high level injector over the concord-bft code to perform various performance related operations.
It's instance can be injected to various modules via constructors, which is default pattern in Concord-bft for dependencies injection.

## SlowdownManager

As the name suggests, this class is responsible for implementing slowdown methods. Sometimes in order to figure out performance bottleneck it makes sense to slow down some components and to evaluate overall system impact.

This class implements `Delay<T>` method, which can be called from various modules.

The `slowdown_config_reference.yaml` file can be used as a reference configuration file. Currently 5 modules are supported, each of them has it's own phases.
This is not a generic mechanism to handle slowdown configuration, but an example of how application can do it.

This file should be managed and parse by the application that uses concord-bft. Class' ctor expects configuration object to be passed from the calling application.

### Policies
Policy defines how the slowdown is reached. We support Sleep, BusyWait and AddKeys policies, each of them can be used in appropriate place in the code. Sleep and BusyWait are trivial and the AddKeys can be used for storage related tasks and to create additional work by adding dummy keys to the write set.

## Build
The class depends on `USE_SLOWDOWN` preprocessor definition, which in turn is set by setting `BUILD_SLOWDOWN` option in the main `CMakeLists.txt` file.
The expectation is that the up level application will set this build flag. 

