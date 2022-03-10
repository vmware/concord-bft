# Overview
This directory contains a simple open source example/demo that we can provide to any user who wants to learn how concord-bft works and how they may use it. 

This example/demo is a byzantine fault tolerant K/V-store implementation which is fully wired system, based on Concord-BFT consensus. It is responsible for assisting and guiding many concord-bft users throughout the world, including those who want to understand more about concord-bft, its usage, and how it works. 
This example/demo will provide end to end functionality of Concord byzantine fault tolerant state machine replication library about its usage and how we can use this as a library to implement a highly scalable and energy-efficient distributed trust infrastructure for consensus and smart contract execution.
So far, we're intending to implement this using an open source replica kvbc, similar to how we do it in Apollo tests, and then, after the setup is complete, we'll include other open source execution engines for this showcase, such as Ethereum, WASM, etc.

## Install and Build (Ubuntu Linux 18.04 or above)
### Pre-requisite
* Latest docker should be installed
* Optional: [configure docker as non-root user](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user).

### Build
```sh
git clone https://github.com/vmware/concord-bft
cd concord-bft
make
```
Run `make help` to see more commands.

#### Note:
* The output binaries are stored in the host's `concord-bft/build`.
* `Makefile` is configurable.
For example, if you want to use another compiler you may pass it to the `make`:
```
make CONCORD_BFT_CONTAINER_CXX=g++ \
    CONCORD_BFT_CONTAINER_CC=gcc \
    build
```
Other build options, including passthrough options for CMake, are defined in the [Makefile](../Makefile) and prefixed with `CONCORD_BFT_`.


## Run Example/Demo
This simple example can be run from the script [test_example.sh](scripts/test_example.sh) under `example/script` directory.

### Simple scenario (4 replicas and 1 client)
This demo can be run from inside the container once the build is finished.

Run the [test_example.sh](scripts/test_example.sh) script from the container's `build/example/scripts` directory.
```sh
./test_example.sh
```

#### Explanation of the [test_example.sh](scripts/test_example.sh)
On our example, we will use the script [test_example.sh](scripts/test_example.sh) and this script is used for following things,
* It is used to generate the keys for replica's. For more [refer](../tools/README.md)
```
$scriptdir/../../tools/GenerateConcordKeys -f 1 -n 4 -o replica_keys_
```

* It is used to generate TLS certificates used in for TLSTCP communication which we are using as a default mode of communication. For more [refer](../scripts/linux/create_tls_certs.sh)
```
# Generate TLS certificates
$scriptdir/create_tls_certs.sh 10
```

* Running multiple replica's. Here we are running 4 replica's.
```
# run 4 replica's with unique replica id's
echo "Running replica 1..."
$scriptdir../replica/test_replica -i 0 -a $scriptdir/replica_conf &
echo "Running replica 2..."
$scriptdir../replica/test_replica -i 1 -a $scriptdir/replica_conf &
echo "Running replica 3..."
$scriptdir../replica/test_replica -i 2 -a $scriptdir/replica_conf &
echo "Running replica 4..."
$scriptdir../replica/test_replica -i 3 -a $scriptdir/replica_conf &
```

* Running client

* For resources cleanup.


## Directory Structure.
- [kv-cmf](./kv-cmf): Byzantine fault-tolerant K/V store messages/commands for the open source state machine example.
- [replica](./replica): The replica's codebase is used to set up and maintain the replica.
	- [include](./replica/include): External interfaces of replica
	- [src](./replica/src): Internal implementation of replica
- [scripts](./scripts): Build scripts, replica config file, etc.


## Future Plans
In future we are planning to include open source execution engines for this demo, such as Ethereum, WASM, etc.


## License
This example/demo part of concord-bft is available under the [Apache 2 license](../LICENSE).