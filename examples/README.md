# Overview
We are providing an open-source illustration and demonstration of example/demo. This will help us to understand the usage of the concord-bft library. Since concord-bft is an open-source consensus, this application will also assist us in comprehending the internals of concord-bft.

It is responsible for assisting and guiding large number of concord-bft users around the globe, including those who want to understand more about concord-bft, its usage, and how it works.

This example/demo will provide end to end functionality of Concord byzantine fault tolerant state machine replication library about its usage. This can be used as a library to build a distributed trust infrastructure that is extremely scalable and low-power for smart contract execution.

**Note:** Initially, we plan to do this using an open source key-value blockchain replica. However, once the setup is finished, we'll also use alternative open source execution engines, such Ethereum, WASM, etc.


## Install and Build (Ubuntu Linux 18.04 or above)
Concord-BFT supports two kinds of builds are,
* Docker
* Native

The docker build is **strongly recommended**


### Docker

* [Install the latest docker] (https://docs.docker.com/engine/install/ubuntu/).
* Optional: [configure docker as non-root user](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user).
* Build concord-bft
```sh
git clone https://github.com/vmware/concord-bft
cd concord-bft
make
```
Run `make help` to see more commands.

Note:
* If you face any format related issues than run `make format` before running `make` command to format the changes.
* The output binaries are stored in the host's `concord-bft/build`.
* Default compiler is clang and clang++.
* `Makefile` is configurable. If you want to use another compiler you may pass it to the `make`.
For example,
```
make CONCORD_BFT_CONTAINER_CXX=g++ \
    CONCORD_BFT_CONTAINER_CC=gcc \
    build
```
Other build options, including passthrough options for CMake, are defined in the [Makefile](../Makefile) and prefixed with `CONCORD_BFT_`.


### Native

For native development environment, need to install some dependencies mentioned in [install_deps.sh](../install_deps.sh) script.
```sh
git clone https://github.com/vmware/concord-bft
cd concord-bft
sudo ./install_deps.sh    # Install all dependencies and 3rd parties
mkdir build
cd build
cmake ..
make
```


## Run Example/Demo
This simple example can be run from the script [test_osexample.sh](scripts/test_osexample.sh) under `example/script` directory.


### Simple scenario (4 replicas and 1 client)
This demo can be run from inside the container once the build is finished.

Run the [test_osexample.sh](scripts/test_osexample.sh) script from the container's `build/example/scripts` directory.
```sh
./test_osexample.sh
```


#### Explanation of the [test_osexample.sh](scripts/test_osexample.sh)
On our example, we will use the script [test_osexample.sh](scripts/test_osexample.sh) and this script is used for following things,
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
$scriptdir/../replica/test_replica -i 0 -a $scriptdir/replica_conf &
echo "Running replica 2..."
$scriptdir/../replica/test_replica -i 1 -a $scriptdir/replica_conf &
echo "Running replica 3..."
$scriptdir/../replica/test_replica -i 2 -a $scriptdir/replica_conf &
echo "Running replica 4..."
$scriptdir/../replica/test_replica -i 3 -a $scriptdir/replica_conf &
```

* Running client
```
time $scriptdir/../client/test_client -f 1 -c 0 -i 4 -r 4 -e 0 -m $scriptdir/../msg-configs/msg-1
```

* For resources cleanup.


## Directory Structure.
- [client](./client): Client codebase used to setup and maintain client. Used to parse message config and create a new client read/write message for the processing.
- [kv-cmf](./kv-cmf): Byzantine fault-tolerant Key-Value messages/commands for the open source state machine example.
- [replica](./replica): The replica's codebase is used to set up and maintain the replica.
- [msg-configs](./msg-configs): Message configurations for read/write messages.
- [scripts](./scripts): Build scripts, replica config file, etc.

## How to use msg configs
Initially, we are planning to do this using an open source key-value blockchain replica.
Message configs are used to set some parameters which will further used to create ClientRequestMsg.
All the message configurations can be found [msg-configs](./msg-configs) folder.

Following are the parameters from msg config file,
* **type**: This is a message type parameters i.e. it shows the type of message Write, Read and Both. According to this parameter client request msg will be created.
* **number_of_operations**: This parameter is used to send number of read/write messages.
* **sender_node_id**: Default client id for a single client
* **req_timeout_milli**: Timeout parameter if request not send.
* **num_of_writes**: Number of writes used to create a KVWriteRequest.
* **num_of_keys_in_readset**: Number of readset used to create a KVWriteRequest. To know more about the KV CMF messages [refer](./kv-cmf/kv_replica_msgs.cmf).


## Future Plans
In future we are planning to include open source execution engines for this demo, such as Ethereum, WASM, etc.


## License
This example/demo part of concord-bft is available under the [Apache 2 license](../LICENSE).