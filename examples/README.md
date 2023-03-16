# Overview
We are providing an open-source illustration and demonstration of example/demo. This will help us to understand the usage of the concord-bft library. Since concord-bft is an open-source consensus, this application will also assist us in comprehending the internals of concord-bft.

It is responsible for assisting and guiding large number of concord-bft users around the globe, including those who want to understand more about concord-bft, its usage, and how it works.

This example/demo will provide end to end functionality of Concord byzantine fault tolerant state machine replication library about its usage. This can be used as a library to build a distributed trust infrastructure that is extremely scalable and low-power for smart contract execution.

**Note:** Initially, we plan to do this using an open source key-value blockchain replica. However, once the setup is finished, we'll also use alternative open source execution engines, such Ethereum, WASM, etc.


## Install and Build
See [Getting Started](https://github.com/eyalrund/concord-bft/wiki/Getting-Started) for instructions.


## Run Example/Demo
This simple example can be run from the script [test_osexample.sh](scripts/test_osexample.sh) under `examples/script` directory.
Or, it can be directly run from python script [test_osexample.py](scripts/test_osexample.py.in).


### Simple scenario (4 replicas and 1 client)
This demo can be run from inside the container once the build is finished.

* Run the [test_osexample.sh](scripts/test_osexample.sh) script from the container's `build/examples/scripts` directory.
```sh
./test_osexample.sh
```
* Run the [test_osexample.py](scripts/test_osexample.py.in) script from the container's `build/examples/scripts` directory with default commandline arguments `-bft n=4,cl=1`, where n is number of replicas and cl is number of client.
It is a configurable script which can be configured according to the need.
```sh
./test_osexample.py -bft n=4,cl=1
```


#### Explanation of the [test_osexample.sh](scripts/test_osexample.sh)
In our example, we will use the script [test_osexample.py](scripts/test_osexample.py.in) and this script is used for the following things,
* It is used to generate the keys for replica's. For more [refer](../tools/README.md)
* It is used to generate TLS certificates used for TLS/TCP communication which we are using as a default mode of communication. For more info [refer](../scripts/linux/create_tls_certs.sh)
* Running multiple replica's. Here we are running 4 replica's
* Running client
* For resources cleanup


## Directory Structure.
- [client](./client): Client codebase used to setup and maintain client. Used to parse message config and create a new client read/write message for the processing.
- [kv-cmf](./kv-cmf): Byzantine fault-tolerant Key-Value messages/commands for the open source state machine example.
- [replica](./replica): The replica's codebase is used to set up and maintain the replica.
- [msg-configs](./msg-configs): Message configurations for read/write messages.
- [scripts](./scripts): Build scripts, replica config file, etc.

## How to use message configs
Initially, we are planning to do this using an open source key-value blockchain replica.
Message configs are used to set some parameters which will further used to create ClientRequestMsg.
All messages configurations can be found [msg-configs](./msg-configs) folder.

Following are the parameters from msg config file,
* **type**: This is a message type parameters i.e. it shows the type of message Write, Read and Both. According to this parameter client request msg will be created.
* **number_of_operations**: This parameter is used to send number of read/write messages.
* **sender_node_id**: Default client id for a single client
* **req_timeout_milli**: Timeout parameter if request not send.
* **num_of_writes**: Number of writes used to create a KVWriteRequest.
* **num_of_keys_in_readset**: Number of readset used to create a KVWriteRequest. To know more about the KV CMF messages [refer](./kv-cmf/kv_replica_msgs.cmf).


## Future Plans
In future we plan to include open source execution engines for this demo, such as Ethereum, WASM, etc.


## License
This example/demo part of concord-bft is available under the [Apache 2 license](../LICENSE).
