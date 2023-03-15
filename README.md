<img src="logoConcord.png"/>

# Concord-BFT: A Distributed Trust Infrastructure

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

BFT-based systems require substantial communication between nodes and, thus, don’t scale well. Project Concord-bft solves this problem by simplifying and streamlining communication between nodes, enabling greater scalability while increasing overall network throughput.

Project Concord’s BFT engine obtains significant scaling improvements via three major advances:

1. It uses a linear communication consensus protocol while many other BFT consensus protocols (including PBFT) require quadratic communication
2. It exploits optimism to provide a common case fast-path execution
3. It uses modern cryptographic algorithms (BLS threshold signatures)

Its implementation is based on the algorithm described in the paper [SBFT: a Scalable Decentralized Trust Infrastructure for
Blockchains](https://arxiv.org/pdf/1804.01626.pdf).

It is designed to be used as a core building block for replicated distributed data stores, and is especially suited to serve as the basis of permissioned Blockchain systems.

For a real-life integration example, please take a look at [Project Concord](https://github.com/vmware/concord), a highly scalable and energy-efficient distributed trust infrastructure for consensus and smart contract execution.

Start with example usage here: https://github.com/vmware/concord-bft/tree/master/examples

## Documentation

See the github [wiki](https://github.com/concord-bft/wiki/) for detailed explanation.

## Community

[Concord-BFT Slack](https://concordbft.slack.com/).

Request a Slack invitation via <concordbft@gmail.com>.

## License

Concord-bft is available under the [Apache 2 license](LICENSE).
