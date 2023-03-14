<img src="logoConcord.png"/>


# Concord-BFT: a Distributed Trust Infrastructure

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![clang-tidy](https://github.com/vmware/concord-bft/workflows/clang-tidy/badge.svg)](https://github.com/vmware/concord-bft/actions?query=workflow%3Aclang-tidy)
[![Build Status](https://github.com/vmware/concord-bft/workflows/Release%20build%20(gcc)/badge.svg)](https://github.com/vmware/concord-bft/actions?query=workflow%3A"Release+build+%28gcc%29")
[![Build Status](https://github.com/vmware/concord-bft/workflows/Debug%20build%20(gcc)/badge.svg)](https://github.com/vmware/concord-bft/actions?query=workflow%3A"Debug+build+%28gcc%29")
[![Build Status](https://github.com/vmware/concord-bft/workflows/Release%20build%20(clang)/badge.svg)](https://github.com/vmware/concord-bft/actions?query=workflow%3A"Release+build+%28clang%29")
[![Build Status](https://github.com/vmware/concord-bft/workflows/Debug%20build%20(clang)/badge.svg)](https://github.com/vmware/concord-bft/actions?query=workflow%3A"Debug+build+%28clang%29")
[![codecoverage](https://github.com/vmware/concord-bft/actions/workflows/codecoverage.yml/badge.svg)](https://github.com/vmware/concord-bft/actions/workflows/codecoverage.yml)

<!-- ![Concored-bft Logo](TBD) -->

<!-- <img src="TODO.jpg" width="200" height="200" /> -->


## Overview

**Concord-bft** is a generic state machine replication library that can handle malicious (byzantine) replicas.

Its implementation is based on the algorithm described in the paper [SBFT: a Scalable Decentralized Trust Infrastructure for
Blockchains](https://arxiv.org/pdf/1804.01626.pdf).

It is designed to be used as a core building block for replicated distributed data stores, and is especially suited to serve as the basis of permissioned Blockchain systems.

For a real-life integration example, please take a look at [Project Concord](https://github.com/vmware/concord), a highly scalable and energy-efficient distributed trust infrastructure for consensus and smart contract execution.


## Releases

We abide by [semantic versioning](https://semver.org/). Public APIs can change
at any time until we reach version 1.0. We will, however, bump the minor version
when a backwards incompatible change is made.

 [v0.5](https://github.com/vmware/concord-bft/releases/tag/v0.5)

## Install and Build (Ubuntu Linux 18.04)

Concord-BFT supports two kinds of builds: native and docker.

The docker build is **strongly recommended**.

### Docker

* Install the latest docker.
* Optional: [configure docker as non-root user](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user).
* Build:
```sh
cd concord-bft
make
make test
```
Run `make help` to see more commands.

Note:
* The output binaries are stored in the host's `concord-bft/build`.
* `Makefile` is configurable.
For example, if you want to use another compiler you may pass it to the `make`:
```
make CONCORD_BFT_CONTAINER_CXX=g++ \
    CONCORD_BFT_CONTAINER_CC=gcc \
    build
```

Other build options, including passthrough options for CMake, are defined in the Makefile and prefixed with `CONCORD_BFT_`. Variables that are capable of being overridden on the commandline are set with the Make conditional operator `?=` and are at the beginning of [Makefile](Makefile). Please check that file for options.

#### Select comm module
One option that is worth calling out explicitly is the communication (transport) library. Transport defaults to TLS and can be configured explicitly by setting the `CONCORD_BFT_CMAKE_TRANSPORT` flag. The flag defaults to **TLS**, but also supports **UDP** and **TCP**. These can be useful because the use of pinned certificates for TLS requires an out of band setup.

See [create_tls_certs.sh](scripts/linux/create_tls_certs.sh) for an example. This script is used in apollo tests. For production usage, an out of band deployment for each replica must be used to avoid revealing private keys to each replica.

### Native

```sh
git clone https://github.com/vmware/concord-bft
cd concord-bft
sudo ./install_deps_release.sh # Installs all dependencies and 3rd parties
mkdir build
cd build
cmake ..
make
sudo make test
```

By default the build chooses the active compiler on the native build platform. In order to force compilation by clang you can use the following command.
```sh
CC=clang CXX=clang++ cmake ..
```

In order to turn on or off various options, you need to change your cmake configuration. This is
done by passing arguments to cmake with a `-D` prefix: e.g. `cmake -DBUILD_TESTING=OFF`. Note that
make must be run afterwards to build according to the configuration. Please see [CMakeLists.txt](CMakeLists.txt) for configurable options.

#### Select comm module
One option that is worth calling out explicitly is the communication (transport) library.

We support both UDP and TCP communication. UDP is the default. In order to
enable TCP communication, build with `-DBUILD_COMM_TCP_PLAIN=TRUE` in the cmake
instructions shown above.  If set, the test client will run using TCP. If you
wish to use TCP in your application, you need to build the TCP module as
mentioned above and then create the communication object using CommFactory and
passing PlainTcpConfig object to it.

We also support TCP over TLS communication. To enable it, change the
`BUILD_COMM_TCP_TLS` flag to `TRUE` in the main CMakeLists.txt file. When
running simpleTest using the testReplicasAndClient.sh - there is no need to create TLS certificates manually. The script will use the `create_tls_certs.sh` (located under the scripts/linux folder) to create certificates. The latter can be used to create TLS files for any number of replicas, e.g. when extending existing tests.

As we used pinned certificates for TLS, the user will have to manually provide these. THey can use the [create_tls_certs.sh](scripts/linux/create_tls_certs.sh) script as an example.


### C++ Linter

The C++ code is statically checked by `clang-tidy` as part of the [CI](https://github.com/vmware/concord-bft/actions?query=workflow%3Aclang-tidy).
<br>To check code before submitting PR, please run `make tidy-check`.

[Detailed information about clang-tidy checks](https://clang.llvm.org/extra/clang-tidy/checks/list.html).


#### (Optional) Python client

The python client is required for running tests. If you do not want to install python, you can
configure the build of concord-bft by running `cmake -DBUILD_TESTING=OFF ..` from the `build`
directory for native builds, and `CONCORD_BFT_CMAKE_BUILD_TESTING=TRUE make` for docker builds.

The python client requires python3(>= 3.5) and trio, which is installed via pip.

    python3 -m pip install --upgrade trio


#### Adding a new dependency or tool

The CI builds and runs tests in a docker container. To add a new dependency or tool, follow the steps below:

* Rebase against master
* In order to add/remove dependencies update the file
  [install_deps_release.sh](https://github.com/vmware/concord-bft/blob/master/install_deps_release.sh)
* Build new release/debug images: `make build-docker-images`
* Check images current version in
  [Makefile](https://github.com/vmware/concord-bft/blob/master/Makefile#L5)
  and
  [Makefile](https://github.com/vmware/concord-bft/blob/master/Makefile#L10)
* Tag the new images (at least one):
  * For release: `docker tag concord-bft:latest concordbft/concord-bft:<version>`
  * For debug: `docker tag concord-bft-debug:latest concordbft/concord-bft-debug:<version>`
  <br>where version is `current version + 1`.
* Update the version in the Makefile
* Make sure that Concord-BFT is built and tests pass with the new images: `RUN_WITH_DEBUG_IMAGE=TRUE make
  clean build test`
* Ask one of the maintainers for a temporary write permission to Docker Hub
  repository(you need to have a [Docker ID](https://docs.docker.com/docker-id/))
* Push the images (at least one):
  * For release: `docker push concordbft/concord-bft:<version>`
  * For debug: `docker push concordbft/concord-bft-debug:<version>`
* Create a PR for the update:
    * The PR must contain only changes related to the updates in the image
    * PR's summary has to be similar to `Docker update to version release=<new version> debug=<new version>`
    * PR's message has to list the changes made in the image content and
      preferably the reason
    * Submit the PR

Important notes:
1. Adding dependencies or tools directly to the `Dockerfile` is strongly not recommended because it breaks the native build support.
2. If any tools are installed during the build but not needed for the actual compilation/debugging/test execution(for example, `git`), please remove them(`Dockerfile` is an example). The reason is that the image is supposed to be as tiny as possible.


## Apollo testing framework


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

## Run examples


### Simple test application (4 replicas and 1 client on a single machine)

Tests are compiled into in the build directory and can be run from anywhere as
long as they aren't moved.

Run the following from the top level concord-bft directory:

   ./build/tests/simpleTest/scripts/testReplicasAndClient.sh

### Using simple test application via Python script

You can use the simpleTest.py script to run various configurations via a simple
command line interface.
Please find more information [here](./tests/simpleTest/README.md)

### Example application
This example demo application shows some capabilities of the Concord-BFT consensus-based byzantine fault-tolerant state machine replication library.
For Concord-BFT users who are interested in learning more about Concord-BFT and its uses, this application offers a demonstration and instruction.
Overall, any blockchain application based on concord-bft consensus may be created using this example application.
<br>

Use the [test_example.sh](example/scripts/test_example.sh) script to run the example application. This script is also used to perform this demo via the command line interface with different configurations.
<br>

Please see [here](example/README.md) for more information about the example/demo application.

## Directory Structure


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

## Contributing


The concord-bft project team welcomes contributions from the community. If you wish to contribute code and you have not
signed our contributor license agreement (CLA), our bot will update the issue when you open a Pull Request. For any
questions about the CLA process, please refer to our [FAQ](https://cla.vmware.com/faq). For more detailed information,
refer to [CONTRIBUTING.md](CONTRIBUTING.md).

## Notes
The library calls `std::terminate()` when it cannot continue in a safe manner.
In that way, users can install a handler that does something different than just calling `std::abort()`.

## Community


[Concord-BFT Slack](https://concordbft.slack.com/).

Request a Slack invitation via <concordbft@gmail.com>.

## License

concord-bft is available under the [Apache 2 license](LICENSE).
