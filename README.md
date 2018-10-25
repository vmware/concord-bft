<img src="logoConcord.png"/>


# Concord-BFT: a Distributed Trust Infrastructure

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)



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

 [v0.5](https://github.com/vmware/concord-bft/releases/tag/v0.5)


Roadmap
----
We plan to release updates in a quarterly schedule. The next releases will include: optimized State transfer modules with rocksdb integration, configuration and key generation tools, generic key-value interface, and execution engine for EVM smart contracts. We also plan to support Windows, OS X, and additional Linux distributions.


Contributing
----

The concord-bft project team welcomes contributions from the community. If you wish to contribute code and you have not
signed our contributor license agreement (CLA), our bot will update the issue when you open a Pull Request. For any
questions about the CLA process, please refer to our [FAQ](https://cla.vmware.com/faq). For more detailed information,
refer to [CONTRIBUTING.md](CONTRIBUTING.md).

Community
----

[Concord-BFT Slack](https://concordbft.slack.com/).

Get Slack invitation via this [link](https://join.slack.com/t/concordbft/shared_invite/enQtNDI4NjYzMTk5ODMxLWI3ZTc0MTQzNzFhZTNmNDRkM2I0YTZkMWZkMDAwYzg4YTA0YzViNjIyZTIzMjk2YjI5NGZkYzdkMTIyODJhZjI) or send request to <concordbft@gmail.com>. 



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


License
----

concord-bft is available under the [Apache 2 license](LICENSE).



Build (Ubuntu Linux)
----
### Dependencies

CMake and clang:

    sudo apt-get install cmake clang

Get GMP (dependency for [RELIC](https://github.com/relic-toolkit/relic)):

    sudo apt-get install libgmp3-dev

Build and install [RELIC](https://github.com/relic-toolkit/relic)

    cd
    git clone https://github.com/relic-toolkit/relic
    cd relic/
    mkdir build/
    cd build/
    cmake -DALLOC=AUTO -DWORD=64 -DRAND=UDEV -DSHLIB=ON -DSTLIB=ON -DSTBIN=OFF -DTIMER=HREAL -DCHECK=on -DVERBS=on -DARITH=x64-asm-254 -DFP_PRIME=254 -DFP_METHD="INTEG;INTEG;INTEG;MONTY;LOWER;SLIDE" -DCOMP="-O3 -funroll-loops -fomit-frame-pointer -finline-small-functions -march=native -mtune=native" -DFP_PMERS=off -DFP_QNRES=on -DFPX_METHD="INTEG;INTEG;LAZYR" -DPP_METHD="LAZYR;OATEP" ..
    make
    sudo make install

Build and install [cryptopp](https://github.com/weidai11/cryptopp)

    cd
    git clone https://github.com/weidai11/cryptopp.git
    cd cryptopp/
    git checkout CRYPTOPP_5_6_5;
    mkdir build/
    cd build/
    cmake ..
    make
    sudo make install
	
Get GNU Parallel

    sudo apt-get install parallel

Get g++:

    sudo apt-get install g++

#### (Optional) Use log4cplus

We have simple console logger but if you wish to use log4cplus - we have an
infra that supports it.

Follow below steps for installing this library:
1. Install prerequisites:

```
    sudo apt-get install autoconf automake
```

2. Clone the repository:

```
    git clone https://github.com/log4cplus/log4cplus.git
```

3. Move to the extracted directory and checkout the appropriate branch:

```
    cd log4cplus
    git checkout REL_1_2_1
```

4. Edit `configure` to change "am__api_version" from 1.14 to 1.15, the
version that ubuntu 16.04 supports.

5. Configure/make/install

```
    ./configure CXXFLAGS="--std=c++11"
    make
    sudo make install
```

Configuring with these flags is important. If log4cplus is build without `c++11` then athena will give linker errors while building.

At this point all library files and header files should be installed into `/usr/local`. (You may need to add `/usr/local/lib` to your `LD_LIBRARY_PATH`).
You may also need to export CPLUS_INCLUDE_PATH variable set to /usr/local/include for the header files.

After installation, set USE_LOG4CPP flag to TRUE in the main CmakeLists.txt . The library doesn't initialize the log4cpp subsystem, including formats and appenders, it expects that the upper level application will do it and the log4cpp subsystem is already initialized.

### Select comm module
We support both UDP and TCP communication (UDP is the default one).
In the main Cmake file please set BUILD_COMM_TCP_PLAIN flag to TRUE to
build TCP - then the test client we use will run using TCP. If you wish to
use TCP in your application, you need to build the TCP module as mentioned
above and then create the communication object using CommFactory
and passing PlainTcpConfig object to it.

### Build concord-bft

    cd
    cd concord-bft

    # NOTE: This will add concord-bft/scripts/linux to PATH
    #
    . scripts/linux/set-env.sh release
    #
    # ...so you can invoke the make script as follows
    #
    make.sh


For debug builds use:

    . scripts/linux/set-env.sh debug



Run examples
----

### Simple test application (4 replicas and 1 client on a single machine)

    export LD_LIBRARY_PATH=/usr/local/lib

Go to the build directory (here `<buildtype>` is either `release` or `debug`)

    cd ~/builds/concord-bft/<build-type>/bftengine/tests/simpleTest/scripts

    ./testReplicasAndClient.sh

    # Or, you can alternatively do: ./runReplicas.sh followed by ./runClient.sh
