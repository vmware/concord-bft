
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

## Building and running the demo with docker-compose

This is the recommended way to build and run the utt demo.

Run the following scripts in order to produce the necessary binaries and docker images for running the demo:
  ```
  ./1_utt-demo-build.sh
  ./2_utt-demo-build-replica-image.sh
  ./3_utt-demo-build-wallet-image.sh
  ./4_utt-demo-build-payment-service-image.sh
  ```
To run the demo first you need to start all services (this includes the concord-bft replicas and payment services)
  ```
  ./5_utt-demo-start.sh
  ```
You can now launch a specific user account in the range 1 to 9 by running the following script:
  ```
  ./6_utt-demo-login-wallet.sh [number]
  ```
  Substitute [number] with a value from 1 to 9. Wallets use payment services as follows:
  * Wallets 1, 2, 3 -> payment-service-1
  * Wallets 4, 5, 6 -> payment-service-2
  * Wallets 7, 8, 9 -> payment-service-3
  
  You can kill a payment service (1 to 3) or a bft replica (1 to 4) manually by calling:
  ```
  docker-compose stop payment-service-[n]
  docker-compose stop replica-[n]
  ```

  If you kill the a primary replica (the initial primary is replica-1) you would cause a view-change and stall the system for some time. This may need some configuration tuning to get the test scenario right.

  To start the service again use:
  ```
  docker-compose start payment-service-[n]
  docker-compose start replica-[n]
  ```
You can stop all services by calling:
  ```
  ./7_utt-demo-stop.sh
  ```

The state of the system will persist until the replica database files are deleted. 
You can stop the system and delete the state in a single command by calling:
  ```
  ./8_utt-demo-reset.sh
  ```

 ## Building and running the demo without docker-compose
 You can build and test the demo without using the provided docker-compose file. This is intended for development and troubleshooting the demo.

 Start by building the binaries:
  ```
  ./1_utt-demo-build.sh
  ```

 Now you need to manually login to the docker container with the build environment:
 ```
 make login
 ```

 Go to the scripts folder:
 ```
 cd build/tests/uttDemo/scripts
 ```

 At this point you can execute the various scripts that mirror the scripts for running the demo with docker-compose:
 ```
 ./startServices.sh
 ./stopServices.sh
 ./startClient.sh [number]
 ./clean.sh
 ```

 # Generating custom UTT config files
 The UTT demo requires a special configuration file for each replica and wallet. The gen_utt_cfg tool is used to create these files based on an input text configuration file. The default utt_config.txt is found in [tests/uttDemo/tools/utt_config.txt](tests/uttDemo/tools/utt_config.txt).
 
 If you want to generate a utt configuration that is different from the default:
 * Copy and edit the utt_config.txt
 * Pass the new config file to build/tests/uttDemo/tools/gen_utt_cfg
 * Copy the produced utt config files to the respective config folder depending on how you run the demo. Note that each time you run '1_utt-demo-build.sh' the default config files from 'tests/uttDemo/scripts/config' will be copied. If you want to change the default pre-generated configuration you need to replace those.
  * With docker-compose - copy the custom config files in utt-demo-run/config
  * Without docker-compose - copy the custom config files is in build/tests/uttDemo/scripts/config

 # Running libutt tests

 To run the libutt tests:
 * Fully build the project by running 'make build'
 * Use 'make login' to start an interactive terminal inside the build environment container.
 * Navigate to the folder 'cd build/utt'
 * Use 'ctest -N' to list the available tests
 * Use 'ctest -V' to run all tests or 'ctest -V -R <regex>' to run a subset/single test
