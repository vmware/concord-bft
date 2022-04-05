
# Concord-BFT UTT Demo

## Overview

This branch of concord-bft contains a demo of UnTracable Transactions (UTT), this is a
cryptographic framework for allowing accountable anonymous payments. The demo has two types of
transactions available:
 * Public transactions - these represent ordinary money transfers visible to the bank.
 * UTT transactions - these represent accountable anonymous transfers which can be either
 fully anonymous to the bank (not knowing who or how much transferred to whom) and can be
 accountable (only a limited amount of money can be transferred anonymously in a given time period).

## Building the demo

To build the demo there are two options:
 * Build all of concord-bft with the 'make' or 'make build' commands.
 * Build only utt demo targets with the build-utt-demo.sh script.

 The latter will be faster, but if you want to run concord-bft tests you will need to fully build the project

 ## Running the demo
 **This is WIP and instructions reflect the current state of running the demo**

 To run the demo you need to use the build environment provided by the docker image:
 * Use 'make login' to start an interactive terminal inside the build environment container.
 * Navigate to the folder 'cd build/tests/uttDemo/scripts/'
 * Generate the utt config files with '../tools/gen_utt_cfg' if they don't exist (this step is soon to be obsolete)
 * Start replicas by sourcing the replicas script '. startReplicas.sh' - this will launch 4 replicas as background processes
 * Start the client with './startClient.sh' - this launches an interactive client with predefined utt accounts to send demo commands.
 * To reset the state of the replicas stop them with './stopReplicas.sh' and do 'rm -rf simpleKVBTests_DB_*'

 # Running libutt tests

 To run the libutt tests:
 * Use 'make login' to start an interactive terminal inside the build environment container.
 * Navigate to the folder 'cd build/utt'
 * Use 'ctest -N' to list the available tests
 * Use 'ctest -V' to run all tests or 'ctest -V -R <regex>' to run a subset/single test
