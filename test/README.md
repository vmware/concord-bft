This directory contains system tests in python as well as a library of reusable
test components.

# Design 

The design behind the system testing strategy is to provide a collection of
reusable components to test any application built on concord-bft, as well as
reusable components for specific application instances like SimpleKVBCTests
TesterReplica. The application indpenendent component is responsible for
infrastructure manipulation like starting and stopping replicas, and creating
network partitions. It also provides reusable functions and methods applicable
to all system tests, such as `wait_for_state_transfer_to_start`. The reusable
application dependent code provides the client wire protocol for interacting
with the specific application state machine and mechanisms for verifying
correctness of operations (such as system models, which are coming in a future
commit). 

Tests specific to application instances are written that verify certain
properties of the system, such that state transfer will start and complete,
resulting in equivalent execution sequence numbers when a node is started after
being offline when a lot of data was added to the other nodes in the system. 

Application indpendent components as well as the system tests themselves rely on
the the `bft_client` and `bft_metrics_client` living in `../util/pyclient`.

## Application Independent Components

 * `BftTester` - Infrastructure code (`bft_tester.py`)
 * `BftMetrics` - Metrics client wrapper code (`bft_metrics.py`)
 * `BftTesterExceptions` - All exceptions for BftTester
   (`bft_tester_exceptions.py`)

## SimpleKVBC specific Compoennts

 * `SimpleKVBCProtocol` - Message constructors and parsers for SimpleKVBC
   messages (`skvbc.py`)
 * `SkvbcTest` - Unittest class containing individual system tests for
   SimpleKVBC (`test_skvbc.py`)

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
executing our tests. We currently only have a single test class for system tests
in `test_skvbc.py`, although our clients each have their own tests in
`../util/pyclient`. 

The tests in `test_skvbc.py` can be run from the test directory via `python3
test_skvbc.py`. They can also be run from the build directory along with every
other automated test by running `make test`.

We also follow [Pep 8](https://www.python.org/dev/peps/pep-0008/) guidelines for code style:
 * Class names are `CamelUpperCase`
 * Method, function, and variable names are `snake_lower_case`
 * Lines are limited to 80 chars 

As a convention to help distinguish public from private methods and fields of a
class, private methods, fields and functions begin with an underscore.

# Writing a test

The most likely scenario is that you want to add a test for a specific
functionality utilizing the SimpleKVBCTest TesterReplicas. For this you'll need
to create at least two methods in the `SkvbcTest` class in `test_skvbc.py`.
Python's unit testing framework treats all methods for subclasses of
`unittest.TestCase` that start with `test` as tests to be run. Each test should
have one `test_XXX` method with a description and a single line that calls
`trio.run(self._test_XXX)`, where `_test_XXX` is an async method, starting with
`async def` that runs the actual test code. This pattern is necessary to write
tests that use coroutines. An example is the `test_state_transfer` and
`_test_state_transfer` methods.

Each test must create a `bft_tester.TestConfig`, and then instantiate a
`bft_tester.BftTester` with the config as a parameter. We specifically 
use the `with` keyword so that if the test (in the scope under the with) fails,
all resources in the tester will be cleaned up automatically by the python
runtime.

From there, `await tester.init()` must be called within the with block. This
performs initialization of async state such as creating sockets using trio. This
separate method is required, since constructors cannot be async functions and
therefore cannot call async functions. Also note the `await` keyword. This is
required when calling async functions, and essentially switches out the current
coroutine, returns control back to the event loop, and then picks up where it
left off when the async function completes. It appears as a blocking call to the
code and can be treated as such when viewing the code as operating linearly.
Note however, that if you leave off the await keyword the code will **not run**.
Instead a coroutine object will be returned, which is definitely not what you
want. Unfortunately, python doesn't generate a compile error here, although it
will warn you in your output if this happens, with a message like: `__main__:4:
RuntimeWarning: coroutine 'sleep' was never awaited`. This is [documented
well](https://trio.readthedocs.io/en/latest/tutorial.html#warning-don-t-forget-that-await) 
in the trio docs.

From there you can create a protocol and start coroutines running in the
background via
[`trio.open_nursery()`](https://trio.readthedocs.io/en/latest/reference-core.html#nurseries-and-spawning).
We aren't going to go into detail about this, as the trio documentation is
excellent and you should read it!. Existing tests should serve as guidelines for
what is possible.

Importantly, each test starts fresh replicas, and should be treated as an empty
blockchain until the test writes data.

