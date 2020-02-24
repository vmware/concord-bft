<img src="logoConcord.png"/>


# Concord-BFT: a Distributed Trust Infrastructure

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.com/vmware/concord-bft.svg?branch=master)](https://travis-ci.com/vmware/concord-bft)



<!-- ![Concored-bft Logo](TBD) -->

<!-- <img src="TODO.jpg" width="200" height="200" /> -->


Overview
----
Project Concord is a highly scalable and energy-efficient distributed trust infrastructure for consensus and smart contract execution.

concord-bft
----


The core engine of project Concord is concord-bft, a generic state machine replication library that can handle malicious (byzantine) replicas.
This library is designed to be used as a core building block for replicated distributed data stores, and is especially suited to serve as the basis of permissioned Blockchain systems.
Its implementation is based on the algorithm described in the paper [SBFT: a Scalable Decentralized Trust Infrastructure for
Blockchains](https://arxiv.org/pdf/1804.01626.pdf).

Releases
----

We abide by [semantic versioning](https://semver.org/). Public APIs can change
at any time until we reach version 1.0. We will, however, bump the minor version
when a backwards incompatible change is made.

 [v0.5](https://github.com/vmware/concord-bft/releases/tag/v0.5)

Contributing
----

The concord-bft project team welcomes contributions from the community. If you wish to contribute code and you have not
signed our contributor license agreement (CLA), our bot will update the issue when you open a Pull Request. For any
questions about the CLA process, please refer to our [FAQ](https://cla.vmware.com/faq). For more detailed information,
refer to [CONTRIBUTING.md](CONTRIBUTING.md).

Community
----

[Concord-BFT Slack](https://concordbft.slack.com/).

Get Slack invitation via this [link](https://join.slack.com/t/concordbft/shared_invite/enQtNTAyMzg0MTk0NzU0LWViMjA4ODQ4OTcxYmVkMWU4NGYyNTZmZDQ0ZTIzZTNiZmIyZmI5Mjg0NDZjNjBkYmI2Yjg1Mzg1NGUyNjQ0OWQ) or send request to <concordbft@gmail.com>.



Directory structure
----

- [bftengine](./bftengine): concord-bft codebase
	- [include](./bftengine/include): external interfaces of concord-bft (to be used by client applications)
	- [src](./bftengine/src): internal implementation of concord-bft
    - [tests](./bftengine/tests): tests and usage examples
- [threshsign](./threshsign): crypto library that supports digital threshold signatures
	- [include](./threshsign/include): external interfaces of threshsign (to be used by client applications)
	- [src](./threshsign/src): internal implementation of threshsign
    - [tests](./threshsign/tests): tests and usage examples
- [scripts](./scripts): build scripts
- [tests](./tests): BFT engine system tests

License
----

concord-bft is available under the [Apache 2 license](LICENSE).



Build (Ubuntu Linux 18.04)
----
We use Conan Package Manager to install all concord-bft dependencies.
Dependencies that currently are not supported by the conan center, have custom conan installer in concord-bft/.conan

### Install Dependencies
Install all dependencies using

    ./install.sh

### Select comm module
We support both UDP and TCP communication. UDP is the default. In order to
enable TCP communication, build with `-DBUILD_COMM_TCP_PLAIN=TRUE` in the cmake
instructions shown below.  If set, the test client will run using TCP. If you
wish to use TCP in your application, you need to build the TCP module as
mentioned above and then create the communication object using CommFactory and
passing PlainTcpConfig object to it.

We also support TCP over TLS communication. To enable it, change the
`BUILD_COMM_TCP_TLS` flag to `TRUE` in the main CMakeLists.txt file. When
running simpleTest using the testReplicasAndClient.sh - there is no need to create TLS certificates manually. The script will use the `create_tls_certs.sh` (located under the scripts/linux folder) to create certificates. The latter can be used to create TLS files for any number of replicas, e.g. when extending existing tests.


### Build concord-bft

Create a build directory and enter it:

    cd concord-bft
    mkdir -p build
    cd build

To perform a default build execute the following:

    conan install --build missing ..
    cmake ..
    make

In order to turn on or off various options, you need to change your cmake configuration. This is
done by passing arguments to cmake with a `-D` prefix: e.g. `cmake -DBUILD_TESTING=OFF`. Note that
make must be run afterwards to build according to the configuration. The following options are
useful for building concord-bft:

 * `CMAKE_BUILD_TYPE`     - Debug | Release | RelWithDebInfo | MinSizeRel (DEFAULT Debug)
 * `BUILD_TESTING`        - OFF | ON  (DEFAULT ON)
 * `BUILD_COMM_TCP_PLAIN` - TRUE | FALSE (DEFAULT FALSE - UDP is used)
 * `BUILD_COMM_TCP_TLS`   - TRUE | FALSE (DEFAULT FALSE - UDP is used)
 * `USE_LOG4CPP`          - TRUE | FALSE (DEFAULT FALSE)
 * `CONCORD_LOGGER_NAME`  - STRING (DEFAULT "concord")

 Note: You can't set both `BUILD_COMM_TCP_PLAIN` and `BUILD_COMM_TCP_TLS` to TRUE.

### (Optional) Python client

The python client is required for running tests. If you do not want to install
python, you can configure the build of concord-bft by running `cmake
-DBUILD_TESTING=OFF ..` from the `build` directory.

The python client requires python3(>= 3.5) and trio, which is installed via pip.

    python3 -m pip install --upgrade trio

Apollo testing framework
----

The Apollo framework provides utilities and advanced testing scenarios for validating 
Concord BFT's correctness properties, regardless of the running application/execution engine.
For the purposes of system testing, we have implemented a "Simple Key-Value Blockchain" (SKVBC) 
test application which runs on top of the Concord BFT consensus engine.
<br>

Apollo enables running all test suites (without modification) against any supported BFT network 
configuration (in terms of <i>n</i>, <i>f</i>, <i>c</i> and other parameters).
<br>

Various crash or byzantine failure scenarios are also covered 
(including faulty replicas and/or network partitioning).
<br>

Apollo test suites run regularly as part of Concord BFT's continuous integration pipeline.

Please find more details about the Apollo framework [here](tests/apollo/README.md)

Run examples
----

### Simple test application (4 replicas and 1 client on a single machine)

Tests are compiled into in the build directory and can be run from anywhere as
long as they aren't moved.

Run the following from the top level concord-bft directory:

   ./build/tests/simpleTest/scripts/testReplicasAndClient.sh

### Using simple test application via Python script

You can use the simpleTest.py script to run various configurations via a simple
command line interface.
Please find more information [here](./tests/simpleTest/README.md)
