# Concord Example Code and Basic Test

In this directory, you'll find a simple test that demonstrates how to
use Concord. The test implements a single-register state machine. The
state machine understands two events:

  * WRITE(value) sets the value in the register, and returns the
    number of times that value has been changed (including this write,
    so the response to the first write will be one).
  * READ just returns the latest value that was set in the register
    (zero if no value has ever been set)

The state machine is defined in the `SimpleAppState` class in
`replica.cpp`. It exposes only an `execute` method, which is called
every time the replica receives an event. The `main` function simply
starts a Concord replica parameterized with this state machine.

The test code is defined in `client.cpp`. The client sends a number of
write requests, followed by a read request, repeatedly. It verifies
that the responses to each of these events matches expectations,
namely that the sequence number advances, and that a read returns the
last written value.

In the `scripts` directory, you'll find:

  * `private_replica_{0,1,2,3}`, which are the key files for each
    replica. The example is for four replicas, which supports an F=1
    environment. The keyfile format used is the same as that output by
    the `GenerateConcordKeys` tool in the `concord-bft/tools` folder;
    see `concord-bft/tools/README.md` for more information on how
    these keyfiles are generated and used.

  * `testReplicasAndClient.sh`, which generates fresh cryptographic
    keys, starts all four replicas, and then runs the client test.

  * `runReplias.sh`, which just starts all four replicas.

  * `runClient.sh`, which just runs the client test.

  * `simpleTest.py`, which allows to run various configurations using command
  line interface.

## Run test using pre configured shell script
  * `create_tls_certs.ssh`, which creates self signed SSL certificates to be used with TlsTcp module.

If you have built Concord with `BUILD_COMM_TCP_TLS TRUE` you must run the following commands to create SSL certificates:

```
rm -rf certs/
./create_tls_certs.sh num_of_replicas+num_of_clients
```
E.g., for running the Simple Test using `testReplicasAndClient.sh, which runs 4 replicas and 1 client, you will need to run
```
rm -rf certs/
./create_tls_certs.sh 5
```

Once you have built Concord and are in the top-level directory, you can run the
test with the following command:

```
./build/bftengine/tests/simpleTest/scripts/testReplicasAndClient.sh
```

You should see output like the following:

```
2018-09-18 11:00:00 91178 91178  INFO       LogInitializer: 121 | Compiler successfully avoids evaluating expressions in 'logtrace << expr()'

 18:00:00.429 INFO: Client 4 - sends request 6447876697453756416 (isRO=0, request size=16, retransmissionMilli=150)
When using programs that use GNU Parallel to process data for publication please cite:

  O. Tange (2011): GNU Parallel - The Command-Line Power Tool,
  ;login: The USENIX Magazine, February 2011:42-47.

This helps funding further development; and it won't cost you a cent.
Or you can get GNU Parallel without this requirement by paying 10000 EUR.

To silence this citation notice run 'parallel --bibtex' once or use '--no-notice'.


 18:00:00.429 INFO: Client 4 - sends request 6447876697453756416 (isRO=0, request size=40,  retransmissionMilli=150, numberOfTransmissions=1, resetReplies=0, sendToAll=1)
 18:00:00.631 INFO: Client 4 - sends request 6447876697453756416 (isRO=0, request size=40,  retransmissionMilli=150, numberOfTransmissions=2, resetReplies=0, sendToAll=1)
 18:00:00.636 INFO: Client 4 received ClientReplyMsg with seqNum=6447876697453756416 sender=1  size=32  primaryId=0 hash=1
 18:00:00.636 INFO: Client 4 received ClientReplyMsg with seqNum=6447876697453756416 sender=0  size=32  primaryId=0 hash=1
 18:00:00.637 INFO: Client 4 received ClientReplyMsg with seqNum=6447876697453756416 sender=2  size=32  primaryId=0 hash=1
 18:00:00.637 INFO: Client 4 - request 6447876697453756416 has committed (isRO=0, request size=16,  retransmissionMilli=150)
...
lots of similar send/receive/commit messages
...
 17:37:11.407 INFO: Client 4 - sends request 6447870955359305728 (isRO=1, request size=8, retransmissionMilli=50)
 17:37:11.407 INFO: Client 4 - sends request 6447870955359305728 (isRO=1, request size=32,  retransmissionMilli=50, numberOfTransmissions=1, resetReplies=0, sendToAll=1)
 17:37:11.407 INFO: Client 4 received ClientReplyMsg with seqNum=6447870955359305728 sender=3  size=32  primaryId=0 hash=657769120
 17:37:11.407 INFO: Client 4 received ClientReplyMsg with seqNum=6447870955359305728 sender=0  size=32  primaryId=0 hash=657769120
 17:37:11.407 INFO: Client 4 received ClientReplyMsg with seqNum=6447870955359305728 sender=1  size=32  primaryId=0 hash=657769120
 17:37:11.407 INFO: Client 4 - request 6447870955359305728 has committed (isRO=1, request size=8,  retransmissionMilli=50)
Client is done, killing 'parallel' at PID 91009

parallel: SIGTERM received. No new jobs will be started.
parallel: Waiting for these 4 jobs to finish. Send SIGTERM again to stop now.
parallel: /home/bfink/builds/concord-bft/debug/bftengine/tests/simpleTest/scripts/../server 3
parallel: /home/bfink/builds/concord-bft/debug/bftengine/tests/simpleTest/scripts/../server 2
parallel: /home/bfink/builds/concord-bft/debug/bftengine/tests/simpleTest/scripts/../server 0
parallel: /home/bfink/builds/concord-bft/debug/bftengine/tests/simpleTest/scripts/../server 1

Killing server processes named '/home/bfink/builds/concord-bft/debug/bftengine/tests/simpleTest/scripts/../server'
```
## Run test with various configurations via Python script

Once you have built Concord and are in the top-level directory, you can run the
test with the following command:

```
./build/bftengine/tests/simpleTest/scripts/simpleTest.py -bft n=4,r=4,f=1,c=0,cl=1
```
The command above is EQUAL to running the test via the shell script, as desribed above.

Note: if you run the command as described above and experience Python related
 issues, please run it using

    python3 build/bftengine/tests/simpleTest/scripts/simpleTest.py -bft n=4,r=4,f=1,c=0,cl=1

### BFT Metadata with the simple test
BFT metadata allows replica to recover from crash if the metadata has been written to the disk when replica is running.
The library itself is not writing anything, leaving it to the application level. However, we have implemented in-memory mock of persistency for this metadata, and the simple test can be run in the mode where replicas perform "soft" start and stop, allowing to simulate failures and then using the metadata to continue running. See the ```-pm``` and ```-rb``` CLI parameters below.

### Command line parameters
The following CLI parameters are supported:

*   ```-bd``` folder where binaries can be found (default is .. )
*   ```-ld``` log folder (default is . )
*   ```-nc``` do not print processes' output to the console (default is False)
*   ```-l``` log level for the script only (not for the binaries). Standard log
levels in capital letters. Default is INFO
*   ```-p``` if present, enables performance measurements. Each client will 
create it's historgram and will print raw latencies, which eventually will be
 displayed as summary histogram, along with throughput and average duration
*   ```-i``` number of transactions each client will send to the replicas
*   ```-vct``` View Change timeout, in milliseconds (default is 60000)
*   ```-bft``` List of BFT configurations to run, separated by white space, each configuration is like n=4,r=4,f=1,c=0,cl=1,testViewChange where:
	*   n is total number of replicas in the system (both running and not)
	*   r is number of actually running replicas (r <= n and r >= 2f+c+1 )
	*   f is max. number of faulty replicas
	*   c is max. number of slow replicas
	*   cl is number of clients to run
	*   testViewChange - if presents, primary replica will be killed after 1/2 of the total transactions been sent
*   ```-pm``` Metadata persistency mode. One of the values of [PersistencyMode enum](https://github.com/vmware/concord-bft/blob/e36b83c34539b77bc7dc0538237909142dff4ed6/bftengine/tests/config/test_parameters.hpp#L32)
*   ```-rb``` Replica behavior for the persistency tests. One of the values of [ReplicaBehavior enum](https://github.com/vmware/concord-bft/blob/e36b83c34539b77bc7dc0538237909142dff4ed6/bftengine/tests/config/test_parameters.hpp#L39)

**IMPORTANT** if n - r >= f and the testViewChange is present, the system will NOT complete the View Change
protocol and will exit after timeout - *THIS TEST WILL BE CONSIDERED AS SUCCESS*

**IMPORTANT** the ```-bft``` parameter values should satisfy n = 3f + 2c + 1, which is a constraint of the consensus algorithm.

### Output
Each BFT configuration run creates a folder named as current timestamp in the log directory (e.g. 02_01_2018_18_32_43).
Log files produced by the replica and client processes will be written in this folder.
The log file from the simpleTest.py itself will be written in the log folder provided to the CLI (```-ld``` parameter), which is current dir by default.

### Examples
The following examples all assume you are running simpleTest.py directly from
`<CONCORD_BFT_DIR>/build/bftengine/tests/simpleTest/scripts`.

Here are a few single test runs that may be useful:

Run 6 out of 6 replicas and 2 clients, with 5000 transactions per client and trigger View Change protocol. This test should succeed.
```
./simpleTest.py -l DEBUG -i 5000 -nc -bft n=6,r=6,f=1,c=1,cl=2,testViewChange
```

Run 15 out of 20 replicas and 4 clients, with 2000 transactions per client. This test should succeed.
```
./simpleTest.py -l DEBUG -i 2000 -nc -bft n=20,r=15,f=5,c=2,cl=4
```

Run 13 out of 20 replicas and 4 clients, with 2000 transactions per client and trigger View Change. This test should succeed, however, View Change protocol will NOT complete since 20 - 13 > 5. In this case, we expect from the View Change NOT to complete and thus time out is a success.
```
./simpleTest.py -l DEBUG -i 2000 -nc -bft n=20,r=13,f=5,c=2,cl=4,testViewChange
```
