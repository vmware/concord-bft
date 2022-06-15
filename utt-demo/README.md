
# Concord-BFT UTT Demo

## Overview

This branch of concord-bft contains a demo of UnTracable Transactions (UTT), this is a
cryptographic framework for allowing accountable anonymous payments. The demo has two types of
transactions available:
 * Public transactions - these represent ordinary money transfers visible to the bank.
 * UTT transactions - these represent accountable anonymous transfers which can be either
 fully anonymous to the bank (not knowing who or how much transferred to whom) and can be
 accountable (only a limited amount of money can be transferred anonymously in a given time period).

## Install docker and docker-compose

* Install docker and docker-compose
  ```
  sudo apt install docker.io docker-compose -y
  ```
* [configure docker as non-root user](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user). This allows you to run the make commands below without sudo.

## Running the demo using docker and make

The easiest way to run the demo is to use the pre-built images from Dockerhub.

You should be in the utt-demo directory. To ge the latest demo images:

```
  cd utt-demo
  make pull
```
Use 'make' or 'make help' to view all available commands for running the demo.

To start the demo (runs all necessary services like concord-bft replicas and payment services) use:
  ```
  make start
  ```

After that you can use any wallet with ID from 1 to 9 by running:
  ```
  make wallet-<ID>
  ```

Each wallet uses a different payment service depending on its number ID:
 * Wallet 1, 2, 3 -> Use Payment Service #1
 * Wallet 4, 5, 6 -> Use Payment Service #2
 * Wallet 7, 8, 9 -> Use Payment Service #3

To reset the demo and start from scratch use:
  ```
  make reset
  ```

You can stop the demo (this stops all running services) with:
  ```
  make stop
  ```

You can kill a payment service (1 to 3) or a replica (1 to 4) manually by calling:
  ```
  docker-compose stop payment-service-[n]
  docker-compose stop replica-[n]
  ```

To start the stopped service again use:
  ```
  docker-compose start payment-service-[n]
  docker-compose start replica-[n]
  ```
## Building demo using docker and make

This is the recommended way to build the utt demo.

Go to the 'utt-demo' directory and run make with the following targets in order to produce the necessary binaries and docker images for running the demo:
  ```
  cd utt-demo
  make build
  make build-docker-all
  ```

## Updating the build and run environment images

Apart from making changes to the demo logic, you might need to produce new versions of the build and run environment images used to build and run the demo.

### Updating the build environment image

The build environment image is based on some version of the concord-bft build image. Usually the demo is pegged at some older version of concord-bft that is stable. Eventually the utt dependencies that need to be added on top of the concord-bft build environment will be folded into it and this step won't be necessary anymore.

Since you're currently in the 'utt' branch of concord-bft, the Makefile at the root of the project is modified to point to the utt version of the build image. You can edit Dockerfile.buildenv and run (using the root Makefile):
  ```
  make build-docker-image
  ```
This will produce a new version of the concord-bft-utt image. Match the tag (version) of the base build image and upload it to Dockerhub if needed.

### Updating the run environment image

The run environment image is based on the build environment image and is essentially a copy of only the required shared libraries and 3rd party installs. This will reduce the size of the final images by ~2GB.

You can modify Dockerfile.runenv and run (using utt-demo/Makefile):
  ```
  make build-docker-runenv
  ```
Tag the produced image respecting the version of the base build image and upload it to Dockerhub if you need to. You also need to produce new versions of the demo images:
  ```
  make build-docker-all
  ```

These will use the latest tag.

## Building and running the demo without docker-compose

You can build and test the demo without needing to build images. This is intended mainly for development and troubleshooting of the demo.

Start by building the binaries (note that this uses the Makefile in 'utt-demo'):
  ```
  cd utt-demo
  make build
  ```

Now go back to the root project directory and use the Makefile there. You need to manually login to the docker container used for building:
  ```
  cd ..
  make login
  ```

Inside the container go to the scripts folder:
  ```
  cd build/tests/uttDemo/scripts
  ```

At this point you can execute the various scripts for running the demo that mirror the Makefile from 'utt-demo':
  ```
  . startServices.sh
  ./stopServices.sh
  ./startClient.sh [number]
  ./clean.sh
  ```
Note that 'startServices.sh' is sourced. This is intentional as you want the service processes to continue operating in the background while you use different clients to test.

### Running automated tests
If you're building the utt demo manually and testing it it's a good idea to run some of the automated tests. These are bash scripts that always do the following:
  1) Start all services and reset the previous state.
  2) Run all wallets and gather their initial state (pids, balances, etc.)
  3) Run the automation - this is dependent on the automation test logic
  4) Run all wallets and gather their final state.
  5) Do simple assertions on the difference between the initial and final state.

After running an automation test the respective artifacts are in the automation folder.

Automated tests:
  * **testRandomTransactions.sh [N]** - each wallet executes N random transactions - this includes minting, burning and transfer of public and utt funds. At the end preservation of balance across all wallets is checked.
  * **testWalletConcurrency.sh** - each wallet executes 150 additions of $1 to its public balance concurrently. The final balance and number of transactions per wallet is checked.

# Generating custom UTT config files

The UTT demo requires a special configuration file for each replica and wallet. The gen_utt_cfg tool is used to create these files based on an input text configuration file. The default utt_config.txt is found in [tests/uttDemo/tools/utt_config.txt](tests/uttDemo/tools/utt_config.txt).

If you want to generate a utt configuration that is different from the default:
  * Copy and edit the utt_config.txt to your liking.
  * Pass the new config file to the config tool 'build/tests/uttDemo/tools/gen_utt_cfg <your-config.txt>'
  * Copy the produced config files to the respective config folder depending on how you run the demo. Note that each time you build the demo with 'make build' the defaults from 'tests/uttDemo/scripts/config' will be copied over. If you want to change the default pre-generated configuration you need to replace those.
  * With docker-compose - copy the custom config files in utt-demo/run/config
  * Without docker-compose - copy the custom config files is in build/tests/uttDemo/scripts/config

# Running libutt tests

To run the libutt tests:
  * Fully build the project by running 'make build' from the Makefile at the root project directory.
  * Use 'make login' to start an interactive terminal inside the build environment container.
  * Navigate to the utt build folder 'cd build/utt'
  * Use 'ctest -N' to list available tests
  * Use 'ctest -V' to run all tests or 'ctest -V -R <regex>' to run tests matching a name pattern.
