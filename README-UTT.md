
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

## Building and running the demo using docker and make

This is the recommended way to build and run the utt demo.

Go to the 'utt-demo' directory and run make with the following targets in order to produce the necessary binaries and docker images for running the demo:
  ```
  cd utt-demo
  make build
  make build-docker-all
  ```

Use 'make' or 'make help' to view the available commands for running the demo.

To start the demo (runs all necessary services like concord-bft replicas and payment services) use:
  ```
  make demo-start
  ```

After that you can use any wallet with ID from 1 to 9 by running:
  ```
  make use-wallet-<ID>
  ```

Each wallet uses a different payment service depending on its number ID:
 * Wallet 1, 2, 3 -> Use Payment Service #1
 * Wallet 4, 5, 6 -> Use Payment Service #2
 * Wallet 7, 8, 9 -> Use Payment Service #3

To reset the demo and start from scratch use:
  ```
  make demo-reset
  ```

You can stop the demo (this stops all running services) with:
  ```
  make demo-stop
  ```

You can kill a payment service (1 to 3) or a replica (1 to 4) manually by calling:
```
docker-compose stop payment-service-[n]
docker-compose stop replica-[n]
```

If you kill the primary replica (the initial primary is replica-1) you would cause a view-change and stall the system for some time. This may need some configuration tuning to get the test scenario right.

To start the service again use:
```
docker-compose start payment-service-[n]
docker-compose start replica-[n]
```

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
  ./startServices.sh
  ./stopServices.sh
  ./startClient.sh [number]
  ./clean.sh
  ```

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
