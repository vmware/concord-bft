<img src="logoConcord.png"/>


# Concord-BFT: a Distributed Trust Infrastructure

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.com/vmware/concord-bft.svg?branch=master)](https://travis-ci.com/vmware/concord-bft)



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

We use the [Conan](https://docs.conan.io/en/latest) package manager to install all concord-bft
dependencies. Dependencies that are currently not supported by conan's central repository, conan
center, have a custom package within concord-bft/.conan.

### Download and Install
Clone the repo and install all dependencies. Note that the install script
will install conan if it does not exist and upgrade it otherwise.

If you do not already have a default profile, one will be created for you in
`~/.conan/profiles/default`. In this case your compiler will be detected and it will default to
building with C++11.

If you already have a default profile, we do not want to edit it for you and break other projects
you may have. Note that in this case you may have to modify your default profile such that it
has the following setting: `compiler.libcxx=libstdc++11`. Please See the conan documentation for
more information on
[profiles](https://docs.conan.io/en/latest/using_packages/using_profiles.html#).

```sh
git clone https://github.com/vmware/concord-bft
cd concord-bft
./install.sh
```

### Build concord-bft

```sh
mkdir -p build
cd build
cmake ..
make
```

### Build Options

In order to turn on or off various options, you need to change your cmake configuration. This is
done by passing arguments to cmake with a `-D` prefix: e.g. `cmake -DBUILD_TESTING=OFF`. Note that
make must be run afterwards to build according to the configuration. The following options are
available:

| Option | Possible Values | Default |
| - | - | - |
| `CMAKE_BUILD_TYPE`     | Debug \| Release \| RelWithDebInfo \| MinSizeRel | Debug |
| `BUILD_TESTING`        | OFF \| ON  | ON |
| `BUILD_COMM_TCP_PLAIN` | TRUE \| FALSE | FALSE - UDP is used |
| `BUILD_COMM_TCP_TLS`   | TRUE \| FALSE | FALSE - UDP is used |
| `USE_LOG4CPP`          | TRUE \| FALSE | FALSE |
| `CONCORD_LOGGER_NAME`  | STRING |"concord" |
| `USE_CONAN`            | OFF\| ON | ON |
| `USE_OPENTRACING`      | OFF\| ON | ON |

 Note(1): You can't set both `BUILD_COMM_TCP_PLAIN` and `BUILD_COMM_TCP_TLS` to TRUE.
 Note(2): In case of setting `USE_CONAN=OFF` cmake will search for packages in their native location.

#### Select comm module
We support both UDP and TCP communication. UDP is the default. In order to
enable TCP communication, build with `-DBUILD_COMM_TCP_PLAIN=TRUE` in the cmake
instructions shown above.  If set, the test client will run using TCP. If you
wish to use TCP in your application, you need to build the TCP module as
mentioned above and then create the communication object using CommFactory and
passing PlainTcpConfig object to it.

We also support TCP over TLS communication. To enable it, change the
`BUILD_COMM_TCP_TLS` flag to `TRUE` in the main CMakeLists.txt file. When
running simpleTest using the testReplicasAndClient.sh - there is no need to create TLS certificates manually. The script will use the `create_tls_certs.sh` (located under the scripts/linux folder) to create certificates. The latter can be used to create TLS files for any number of replicas, e.g. when extending existing tests.

#### (Optional) Python client

The python client is required for running tests. If you do not want to install
python, you can configure the build of concord-bft by running `cmake
-DBUILD_TESTING=OFF ..` from the `build` directory.

The python client requires python3(>= 3.5) and trio, which is installed via pip.

    python3 -m pip install --upgrade trio

### Adding a new source dependency with conan

We use conan for dependencies. If a binary dependency exists on conan-center you may wish to use
it. You can check for this with the following command:

```
conan search -r conan-center <LIBRARY NAME>
```

If the package is present, you can add it to
[conanfile.txt](https://github.com/vmware/concord-bft/blob/master/conanfile.txt) under the
`[requires]` table.

If the package is not present, you will need to create a package. The simplest way is to add a
recipe for downloading from source control and building the source. This is the mechanism
described here. More advanced conan usage is possible, but out of scope of this documentation. We
also do not cover creating a package, just the guidelines for including a new dependency in
concord-bft.

First, create a folder for your package with the suffix `_pkg` in the [`.conan`
directory](https://github.com/vmware/concord-bft/tree/master/.conan)

```
cd .conan
mkdir <SOME_DEPENDENCY>_pkg
```

Second, create your `conanfile.py`. The simplest way to do this is to just copy an existing one
from another package and modify it. For specifics, please see the [conan
documentation](https://docs.conan.io/en/latest/creating_packages.html)

Third, Add the dependency and its version to
[conanfile.txt](https://github.com/vmware/concord-bft/blob/master/conanfile.txt) under the
`requires` table.

Fourth, create a corresponding cmake find file. The easiest thing is to just copy one of the
[existing files](https://github.com/vmware/concord-bft/tree/master/.conan/cmake_helpers) and
modify it for your new package.

After this, you should be able to rerun `cmake` and `make`, and your package should get built and
installed.

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

## Community


[Concord-BFT Slack](https://concordbft.slack.com/).

Request a Slack invitation via <concordbft@gmail.com>.

## License

concord-bft is available under the [Apache 2 license](LICENSE).

