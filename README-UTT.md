
# Concord-BFT UTT Demo

## Overview

This branch of concord-bft contains a demo of UnTracable Transactions (UTT), this is a
cryptographic framework for allowing accountable anonymous payments. The demo has two types of
transactions available:
 * Public transactions - these represent ordinary money transfers visible to the bank.
 * UTT transactions - these represent accountable anonymous transfers which can be either
 fully anonymous to the bank (not knowing who or how much transferred to whom) and can be
 accountable (only a limited amount of money can be transferred anonymously in a given time period).

 ## Installing docker
 
 All build dependencies are provided in a pre-build docker image. This is the recommended way to
 build concord-bft and the utt demo.

 * Install docker 'sudo apt install docker.io'
 * [configure docker as non-root user](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user). This allows you to run the make commands below without sudo.

## Building the demo

To build the demo executables:
 * Run '1_utt-demo-build.sh'
 * If you want to run any tests (apollo, libutt, etc.) you need to build the whole project by running 'make build' or build only the targets you need using the Makefile.

To build the demo for use with docker-compose:
 * Build the executables from the previous step.
 * Run '2_utt-demo-build-client-image.sh'
 * Run '3_utt-demo-build-replica-image.sh'
 
 You can now proceed to run the demo using docker-compose.

 ## Running the demo (WIP)

 To run the demo without docker-compose you need to use the build environment provided by the docker image:
 * Use 'make login' to start an interactive terminal inside the build environment container.
 * Navigate to the folder 'cd build/tests/uttDemo/scripts/'
 * Generate the utt config files with '../tools/gen_utt_cfg' if they don't exist (this step is soon to be obsolete)
 * Start replicas by sourcing the replicas script '. startReplicas.sh' - this will launch 4 replicas as background processes
 * Start the client with './startClient.sh' - this launches an interactive client with predefined utt accounts to send demo commands.
 * To reset the state of the replicas stop them with './stopReplicas.sh' and do 'rm -rf simpleKVBTests_DB_*'

To run the demo using docker-compose:
  * Run '4_utt-demo-start-replicas.sh' to create all service containers and start the replicas.
  * Run '5_utt-demo-start-client.sh' to start the interactive client.
  * Run '6_utt-demo-stop-replicas.sh' to stop all replicas
  * There's no special script but you can restart select replicas by first stopping the chosen service and starting it again. (Note that if you stop the primary 'replica1' the system will become unresponsive due to the view-change protocol. A faster view-change may need to be configured to make this scenario reasonably testable.)
  * Run '7_utt-demo-reset.sh' to stop all services and clean the replica database files and logs.

 # Running libutt tests

 To run the libutt tests:
 * Use 'make login' to start an interactive terminal inside the build environment container.
 * Navigate to the folder 'cd build/utt'
 * Use 'ctest -N' to list the available tests
 * Use 'ctest -V' to run all tests or 'ctest -V -R <regex>' to run a subset/single test
