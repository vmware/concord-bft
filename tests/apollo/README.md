This directory contains Apollo - the Concord BFT engine's system testing framework

# Design

The design behind the system testing strategy is to provide a collection of
reusable components to test any application built on concord-bft, as well as
reusable components for specific application instances like SimpleKVBC
TesterReplica. The application independent component is responsible for
infrastructure manipulation like starting and stopping replicas, and creating
network partitions. It also provides reusable functions and methods applicable
to all system tests, such as `wait_for_state_transfer_to_start`. The reusable
application dependent code provides the client wire protocol for interacting
with the specific application state machine and mechanisms for verifying
correctness of concurrent operations.

Tests specific to application instances are written that verify certain
properties of the system. For example: state transfer will start and complete,
resulting in equivalent execution sequence numbers when a node is started after
being offline when a lot of data was added to the other nodes in the system.

Application indpendent components as well as the system tests themselves rely on
the the `bft_client` and `bft_metrics_client` living in `../pyclient`.

## Application Independent Components

 * `BftTestNetwork` - Infrastructure code (`bft.py`)
 * `BftMetrics` - Metrics client wrapper code (`bft_metrics.py`)

 All exceptions for BftTestNetwork live in `bft_test_exceptions.py`

## SimpleKVBC specific Components

 * `SimpleKVBCProtocol` - Message constructors and parsers for SimpleKVBC
   messages (`skvbc.py`)
 * `SkvbcTracker` - Code that is used to track concurrent requests and respones
   and verify linearizability of operations matches the blockchain state
   (`skvbc_history_tracker.py`).

All exceptions for skvbc live in `skvbc_exceptions.py`


# Libraries and Conventions

To properly test concord-bft, as well as shorten the running time of the tests,
we need the ability to send concurrent operations to replicas. Thankfully, as of
python 3.5, python now provides native coroutines with async/await syntax. This
allows us to write our test code in a linear manner, without resorting to
callbacks, yet still have operations proceed concurrently. The chief problem
with coroutines is ensuring that they can be controlled in an understandable
manner, such that when an error occurs the rest are cancelled if necessary. To
help with usability and safety guarantees, we use
[trio](https://trio.readthedocs.io/en/latest/) for running and managing our
coroutines. Trio's design is extraordinarily well thought out and provides
excellent, and simple, mechanisms for timeouts, cancellation, and coordination
among coroutines. It is essential that anyone reading or writing these tests
become familiar with trio by reading the documentation. Further information
about python native coroutines with async/await syntax, which may help
understanding from a deeper perspective, can be found in [PEP
492](https://www.python.org/dev/peps/pep-0492/).

In addition to coroutines with async/await syntax, we also rely heavily on
Python's RAII mechanism, [context
managers](https://docs.python.org/3/reference/datamodel.html#context-managers).
This allows us to clean up after ourselves without leaking resources like
sockets, via the use of `with` statements. All classes that create resources
that must be destroyed should have a context manager, although they may still be
used without with statements, as long as the `__exit__` method is called when
cleanup is needed (i.e. the object is ready for destruction/garbage collection).

Lastly, we use python's builtin [unit testing
framework](https://docs.python.org/3/library/unittest.html) for writing and
executing our tests. We currently have a number of system test suites focusing
on various properties of the BFT engine and the SKVBC application, such as 
fast/slow commit path, view change, linearizability, persistence among others.
Those tests reside in `test_skvbc_*.py` modules. 
The Python BFT client tests are located under `../util/pyclient`.

The tests in `test_skvbc.py` (or any other test module) can be run from the `tests/apollo` directory via 
`sudo python3 -m unittest test_skvbc`. They can also be run from the build directory along with every
other automated test by running `sudo make test` or `sudo ctest`.

We also follow [Pep 8](https://www.python.org/dev/peps/pep-0008/) guidelines for code style:
 * Class names are `CamelUpperCase`
 * Method, function, and variable names are `snake_lower_case`
 * Lines are limited to 80 chars

As a convention to help distinguish public from private methods and fields of a
class, private methods, fields and functions begin with an underscore.

The built in
[__repr__](https://docs.python.org/3/reference/datamodel.html#object.__repr__)
method is used for data and exception classes to return printable information.
Right now it's human readable and not bound to python types, but we may want to
change that and add `__str__` methods later.

# Writing a test

The most likely scenario is that you want to add a test for a specific
functionality utilizing the `SimpleKVBC/TesterReplica`. For this you'll need
to create new methods in one of the `SkvbcTest*` classes in the
respective `test_skvbc*.py` module (or create a new module for features not covered so far).
Python's unit testing framework treats all methods for subclasses of
`unittest.TestCase` that start with `test` as tests to be run. Each test should
be implemented in a separate `test_XXX` **async** method with a description of the test scenario. 
In order to run test coroutines using trio, the `@with_trio` decorator needs to be added to each test method.

Most tests will need a fresh `bft.BftTestNetwork`, which can be obtained easily by adding
the `@with_bft_network` decorator. This decorator takes care of instantiating a `bft.BftTestNetwork` 
object and passing it into a `bft_network` parameter of the decorated test method.
The `@with_bft_network` decorator also initializes and cleans-up the `bft.BftTestNetwork` instance,
after the decorated test method has completed.
The decorator has two parameters:
* (mandatory) `start_replica_cmd` - a function containing the command(s) for starting an individual replica 
(usually a process, but we could also imagine replica containers at some point)
* (optional) `selected configs` - a lambda used for filtering out relevant BFT network configurations

Here is an example:

```python
@with_trio
@with_bft_network(start_replica_cmd)
async def test_get_block_data(self, bft_network):
    # test logic
    pass     
``` 

or 

```python
@with_trio
@with_bft_network(start_replica_cmd,
                  selected_configs=lambda n, f, c: c >= 1)
async def test_fast_path_resilience_to_crashes(self, bft_network):
    # test logic
    pass
``` 


From there you can create a `SimpleKVBCProtocol` and start coroutines running in the
background via
[`trio.open_nursery()`](https://trio.readthedocs.io/en/latest/reference-core.html#nurseries-and-spawning).
We aren't going to go into detail about this, as the trio documentation is
excellent and you should read it!. Existing tests should serve as guidelines for
what is possible.

Importantly, each test starts fresh replicas, and should be treated as an empty
blockchain until the test writes data.

# A note on invoking coroutines

Successfully invoking a coroutine requires the `await` keyword. This is used as an execution 
"checkpoint" for non-preemptive scheduling of coroutines. Essentially `await` switches out the current
coroutine, returns control back to the event loop, and then picks up where it
left off when the async function completes. It appears as a blocking call to the
code and can be treated as such when viewing the code as operating linearly.
Note however, that if you leave off the `await` keyword the code will **not run**.
Instead a coroutine object will be returned, which is definitely not what you
want. Unfortunately, python doesn't generate a compile error here, although it
will warn you in your output if this happens, with a message like: `__main__:4:
RuntimeWarning: coroutine 'sleep' was never awaited`. This is [documented
well](https://trio.readthedocs.io/en/latest/tutorial.html#warning-don-t-forget-that-await)
in the trio docs.

#Causality tracking using Eliot logs

## Motivation
Apollo's tests contain lots of concurrency, it can be hard to track down why exactly a test failed,
and what the partially ordered sequences of events that triggered the failure.
We want to be able to track which test operations are causally related,
so we can determine which code paths lead to the actual failures.
While most failures in Apollo arise from exceptions, those exceptions are just proximate causes.
We'd like to know what chain of events actually led to the raising of those exceptions in the first place.
That's why we chose Eliot logging framework.

## Overview
Eliot provides a Python logging system that "helps the user to understand" what and why things happened in the application,
and who called what.
The system outputs a JSON format log file, another tool called Eliot-tree can produce from the JSON file,
logs that can be reconstructed into a tree.
This tree tells a story compiled of the events occurred,
and help the user to understand the relations between the events documented in the logs.

## Install Eliot components locally
```shell script
python3 -m pip install --upgrade eliot
python3 -m pip install --upgrade eliot-tree
```

## Log location
Eliot's logs will be printed to the console as well as to a file /apollo/logs directory.
A log file will be generated for each test under his name.

## Commands examples


### Adding logs

* Basic logging an action:

    ```python
    with start_action(action_type=u"store_data"):
        x = get_data()
        store_data(x)
    ```
  
* Logging an action - integration with Trio:

    ```python
    async def say(message, delay):
      with start_action(action_type="say", message=message):
        await trio.sleep(delay)

    async def main():
        with start_action(action_type="main"):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(say, "hello", 1)
                nursery.start_soon(say, "world", 2)
    ```
  
* Logging in an action context:

    ```python
    def func(self):
      with start_action(action_type="func") as action:
        action.log(message_type="mymessage")
    ```
  
* Logging out of an action context:
    
    ```python
    def other_func(x):
      log_message(message_type="other_func")
    ```
  
* Logging functions input/output:

    ```python
    @log_call
    def calculate(x, y):
      return x * y
    ```
  
### Render and filter structured logs

* Generate tree like structure

    ```shell script
    eliot-tree <log_file_name>
    ```
  
* Generate compact one-message-per-line format with local timezone

    ```shell script
    cat <log_file_name> | eliot-prettyprint --compact --local-timezone
    ```
  
* Select tasks after an ISO8601 date-time e.g. "2020-09-21 13:24:21"

    ```shell script
    eliot-tree <log_file_name> --start <YYYY-MM-DDT hh:mm:ss>
    ```
 
 * Select tasks before an ISO8601 date-time
 
    ```shell script
    eliot-tree <log_file_name> --end <YYYY-MM-DDT hh:mm:ss>
    ```

### Using perf to generate stack samples and flame graphs

In order to make performance analysis by stack sampling, the following environment
variables have to be provided to the selected test suite:

* KEEP_APOLLO_LOGS=true 
    True is the default value for this variable, this will create a separate folder for each test to
    store the logs of replicas and if specified also the perf.data with stack samples from the replica
    specified with the next parameter. 
* PERF_REPLICA=r
    This variable specifies which replica id (r) perf will be attached to in order to generate samples.
* PERF_SAMPLES=s
    This variable is optional and specifies the sampling rate (s). If not provided we sample at 1000 hertz.

