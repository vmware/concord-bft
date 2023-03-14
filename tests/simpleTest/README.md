# Concord Example Code and Basic Test

In this directory, you'll find a simple test that demonstrates how to
use Concord. SimpleTest gets compiled along with concord-bft using 'make'
or 'make build' command.
The test implements a single-register state machine. The
state machine understands two events:

  * WRITE(value) sets the value in the register, and returns the
    number of times that value has been changed (including this write,
    so the response to the first write will be one).
  * READ just returns the latest value that was set in the register
    (zero if no value has ever been set)

The state machine is defined in the `SimpleAppState` class in
`simple_app_state.hpp`. It exposes only an `execute` method, which is called
every time the replica receives an event. The `main` function simply
starts a Concord replica parameterized with this state machine.

The test code is defined in `client.cpp`. The client sends a number of
write requests, followed by a read request, repeatedly. It verifies
that the responses to each of these events matches expectations,
namely that the sequence number advances, and that a read returns the
last written value. By default, number of requests is set to 4600 and
log is generated after every 100 requests.

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

Once you have built Concord and are in the top-level directory, you can run the Docker container
with the following command:

```
make login
```

Then, you can run the test using the following command:

```
./build/bftengine/tests/simpleTest/scripts/testReplicasAndClient.sh
```

You should see output like the following:

```
root@9cf36e02c374:/concord-bft# ./build/tests/simpleTest/scripts/testReplicasAndClient.sh
Starting SimpleTest...
Generating SSL certificates for TlsTcp communication...
processing replica 0/4
salt=F083FF33FF7F0000
key=15EC11A047F630CA00F65C25F0B3BFD89A7054A5B9E2E3CDB6A772A58251B4C2
iv =38106509F6528FF859C366747AA04F21
salt=5006A159FE7F0000
key=15EC11A047F630CA00F65C25F0B3BFD89A7054A5B9E2E3CDB6A772A58251B4C2
iv =38106509F6528FF859C366747AA04F21
processing replica 1/4
salt=F0C0CD51FC7F0000
key=15EC11A047F630CA00F65C25F0B3BFD89A7054A5B9E2E3CDB6A772A58251B4C2
iv =38106509F6528FF859C366747AA04F21
salt=C0494EF0FE7F0000
key=15EC11A047F630CA00F65C25F0B3BFD89A7054A5B9E2E3CDB6A772A58251B4C2
iv =38106509F6528FF859C366747AA04F21
processing replica 2/4
salt=809C0851FF7F0000
key=15EC11A047F630CA00F65C25F0B3BFD89A7054A5B9E2E3CDB6A772A58251B4C2
iv =38106509F6528FF859C366747AA04F21
salt=00C59B6CFE7F0000
key=15EC11A047F630CA00F65C25F0B3BFD89A7054A5B9E2E3CDB6A772A58251B4C2
iv =38106509F6528FF859C366747AA04F21
processing replica 3/4
salt=9010C1FAFF7F0000
key=15EC11A047F630CA00F65C25F0B3BFD89A7054A5B9E2E3CDB6A772A58251B4C2
iv =38106509F6528FF859C366747AA04F21
salt=800A5D27FC7F0000
key=15EC11A047F630CA00F65C25F0B3BFD89A7054A5B9E2E3CDB6A772A58251B4C2
iv =38106509F6528FF859C366747AA04F21
processing replica 4/4
salt=F01C727FFE7F0000
key=15EC11A047F630CA00F65C25F0B3BFD89A7054A5B9E2E3CDB6A772A58251B4C2
iv =38106509F6528FF859C366747AA04F21
salt=D0C37FB9FC7F0000
key=15EC11A047F630CA00F65C25F0B3BFD89A7054A5B9E2E3CDB6A772A58251B4C2
iv =38106509F6528FF859C366747AA04F21
Generating new keys...
2022-06-01T06:21:58,238,+0000|INFO ||simpletest.client||||client.cpp:155|SimpleClientParams: clientInitialRetryTimeoutMilli: 150, clientMinRetryTimeoutMilli: 50, clientMaxRetryTimeoutMilli: 1000, clientSendsRequestToAllReplicasFirstThresh: 2, clientSendsRequestToAllReplicasPeriodThresh: 2, clientPeriodicResetThresh: 30
2022-06-01T06:21:58,239,+0000|INFO ||simpletest.client||||simple_test_client.hpp:74|ClientParams: clientId: 4, numOfReplicas: 4, numOfClients: 1, numOfIterations: 4600, fVal: 1, cVal: 0
2022-06-01T06:21:58,239,+0000|INFO ||communication.factory||||CommFactory.cpp:55|Replica communication protocol= TlsTcp, Host=0.0.0.0, Port=3718
2022-06-01T06:21:58,239,+0000|INFO ||concord-bft.tls.runner||||TlsRunner.cpp:33|Starting TLS Runner
2022-06-01T06:21:58,239,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:45|Starting connection manager for 4
2022-06-01T06:21:58,239,+0000|INFO ||simpletest.client||||simple_test_client.hpp:100|Starting 4600
2022-06-01T06:21:58,240,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:307|Resolved node 0: 127.0.0.1:3710 to 127.0.0.1:3710
2022-06-01T06:21:58,241,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:307|Resolved node 1: 127.0.0.1:3712 to 127.0.0.1:3712
2022-06-01T06:21:58,241,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:307|Resolved node 2: 127.0.0.1:3714 to 127.0.0.1:3714
2022-06-01T06:21:58,241,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:307|Resolved node 3: 127.0.0.1:3716 to 127.0.0.1:3716
2022-06-01T06:21:58,241,+0000|WARN ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:326|Failed to connect to node 0: 127.0.0.1:3710 : Connection refused
2022-06-01T06:21:58,241,+0000|WARN ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:329|socket closed
2022-06-01T06:21:58,242,+0000|WARN ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:326|Failed to connect to node 1: 127.0.0.1:3712 : Connection refused
2022-06-01T06:21:58,242,+0000|WARN ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:329|socket closed
2022-06-01T06:21:58,242,+0000|WARN ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:326|Failed to connect to node 2: 127.0.0.1:3714 : Connection refused
2022-06-01T06:21:58,242,+0000|WARN ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:329|socket closed
2022-06-01T06:21:58,242,+0000|WARN ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:326|Failed to connect to node 3: 127.0.0.1:3716 : Connection refused
2022-06-01T06:21:58,242,+0000|WARN ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:329|socket closed
2022-06-01T06:21:58,243,+0000|WARN ||concord.bft.client||||SimpleClientImp.cpp:145|The system is not ready: connectedReplicasNum=0 numberOfRequiredReplicas=3
2022-06-01T06:21:58,243,+0000|WARN ||concord.bft.client||||SimpleClientImp.cpp:336|The system is not ready yet to handle requests => reject reqSeqNum: 6937649425107910656, clientId_: 4, cid: , reqTimeoutMilli: 18446744073709551615
2022-06-01T06:21:58,244,+0000|WARN ||concord.bft.client||||SimpleClientImp.cpp:145|The system is not ready: connectedReplicasNum=0 numberOfRequiredReplicas=3
2022-06-01T06:21:58,244,+0000|WARN ||concord.bft.client||||SimpleClientImp.cpp:336|The system is not ready yet to handle requests => reject reqSeqNum: 6937649425124687872, clientId_: 4, cid: , reqTimeoutMilli: 18446744073709551615
2022-06-01T06:21:58,244,+0000|WARN ||concord.bft.client||||SimpleClientImp.cpp:145|The system is not ready: connectedReplicasNum=0 numberOfRequiredReplicas=3
2022-06-01T06:21:58,244,+0000|WARN ||concord.bft.client||||SimpleClientImp.cpp:336|The system is not ready yet to handle requests => reject reqSeqNum: 6937649425128882176, clientId_: 4, cid: , reqTimeoutMilli: 18446744073709551615
2022-06-01T06:21:58,245,+0000|WARN ||concord.bft.client||||SimpleClientImp.cpp:145|The system is not ready: connectedReplicasNum=0 numberOfRequiredReplicas=3
2022-06-01T06:21:58,245,+0000|WARN ||concord.bft.client||||SimpleClientImp.cpp:336|The system is not ready yet to handle requests => reject reqSeqNum: 6937649425133076480, clientId_: 4, cid: , reqTimeoutMilli: 18446744073709551615
2022-06-01T06:21:58,246,+0000|WARN ||concord.bft.client||||SimpleClientImp.cpp:145|The system is not ready: connectedReplicasNum=0 numberOfRequiredReplicas=3
2022-06-01T06:21:58,246,+0000|WARN ||concord.bft.client||||SimpleClientImp.cpp:336|The system is not ready yet to handle requests => reject reqSeqNum: 6937649425133076481, clientId_: 4, cid: , reqTimeoutMilli: 18446744073709551615
2022-06-01T06:21:58,247,+0000|WARN ||concord.bft.client||||SimpleClientImp.cpp:145|The system is not ready: connectedReplicasNum=0 numberOfRequiredReplicas=3
2022-06-01T06:21:58,247,+0000|WARN ||concord.bft.client||||SimpleClientImp.cpp:336|The system is not ready yet to handle requests => reject reqSeqNum: 6937649425137270784, clientId_: 4, cid: , reqTimeoutMilli: 18446744073709551615
Academic tradition requires you to cite works you base your article on.
When using programs that use GNU Parallel to process data for publication
please cite:

  O. Tange (2011): GNU Parallel - The Command-Line Power Tool,
  ;login: The USENIX Magazine, February 2011:42-47.

This helps funding further development; AND IT WON'T COST YOU A CENT.
If you pay 10000 EUR you should feel free to use GNU Parallel without citing.

To silence this citation notice: run 'parallel --citation'.

2022-06-01T06:21:59,241,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:307|Resolved node 0: 127.0.0.1:3710 to 127.0.0.1:3710
2022-06-01T06:21:59,242,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:307|Resolved node 1: 127.0.0.1:3712 to 127.0.0.1:3712
2022-06-01T06:21:59,243,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:307|Resolved node 2: 127.0.0.1:3714 to 127.0.0.1:3714
2022-06-01T06:21:59,245,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:307|Resolved node 3: 127.0.0.1:3716 to 127.0.0.1:3716
2022-06-01T06:21:59,245,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:334|Connected to node 0: 127.0.0.1:3710
2022-06-01T06:21:59,247,+0000|INFO ||concord-bft.tls.conn||||AsyncTlsConnection.cpp:311|Certificates Path: "certs/4/client/client.cert"
2022-06-01T06:21:59,248,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:334|Connected to node 1: 127.0.0.1:3712
2022-06-01T06:21:59,248,+0000|INFO ||concord-bft.tls.conn||||AsyncTlsConnection.cpp:311|Certificates Path: "certs/4/client/client.cert"
2022-06-01T06:21:59,248,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:334|Connected to node 2: 127.0.0.1:3714
2022-06-01T06:21:59,249,+0000|INFO ||concord-bft.tls.conn||||AsyncTlsConnection.cpp:311|Certificates Path: "certs/4/client/client.cert"
2022-06-01T06:21:59,249,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:334|Connected to node 3: 127.0.0.1:3716
2022-06-01T06:21:59,249,+0000|INFO ||concord-bft.tls.conn||||AsyncTlsConnection.cpp:311|Certificates Path: "certs/4/client/client.cert"
2022-06-01T06:21:59,259,+0000|INFO ||concord.bft||||crypto_utils.cpp:314|Peer: 0
2022-06-01T06:21:59,259,+0000|INFO ||concord.bft||||crypto_utils.cpp:319|Field CN: node0ser
2022-06-01T06:21:59,259,+0000|INFO ||concord.bft||||crypto_utils.cpp:314|Peer: 0
2022-06-01T06:21:59,259,+0000|INFO ||concord.bft||||crypto_utils.cpp:319|Field CN: node0ser
2022-06-01T06:21:59,264,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:234|Client handshake succeeded for peer 0
2022-06-01T06:21:59,266,+0000|INFO ||concord.bft||||crypto_utils.cpp:314|Peer: 3
2022-06-01T06:21:59,266,+0000|INFO ||concord.bft||||crypto_utils.cpp:319|Field CN: node3ser
2022-06-01T06:21:59,266,+0000|INFO ||concord.bft||||crypto_utils.cpp:314|Peer: 3
2022-06-01T06:21:59,266,+0000|INFO ||concord.bft||||crypto_utils.cpp:319|Field CN: node3ser
2022-06-01T06:21:59,268,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:234|Client handshake succeeded for peer 3
2022-06-01T06:21:59,270,+0000|INFO ||concord.bft||||crypto_utils.cpp:314|Peer: 2
2022-06-01T06:21:59,270,+0000|INFO ||concord.bft||||crypto_utils.cpp:319|Field CN: node2ser
2022-06-01T06:21:59,270,+0000|INFO ||concord.bft||||crypto_utils.cpp:314|Peer: 2
2022-06-01T06:21:59,270,+0000|INFO ||concord.bft||||crypto_utils.cpp:319|Field CN: node2ser
2022-06-01T06:21:59,274,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:234|Client handshake succeeded for peer 2
2022-06-01T06:21:59,275,+0000|INFO ||concord.bft||||crypto_utils.cpp:314|Peer: 1
2022-06-01T06:21:59,275,+0000|INFO ||concord.bft||||crypto_utils.cpp:319|Field CN: node1ser
2022-06-01T06:21:59,275,+0000|INFO ||concord.bft||||crypto_utils.cpp:314|Peer: 1
2022-06-01T06:21:59,275,+0000|INFO ||concord.bft||||crypto_utils.cpp:319|Field CN: node1ser
2022-06-01T06:21:59,277,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:234|Client handshake succeeded for peer 1
Iterations count: 100
Total iterations count: 100
Iterations count: 100
Total iterations count: 200
Iterations count: 100
Total iterations count: 300
Iterations count: 100
Total iterations count: 400
Iterations count: 100
Total iterations count: 500
Iterations count: 100
Total iterations count: 600
...
...
...
Total iterations count: 4400
Iterations count: 100
Total iterations count: 4500
Iterations count: 100
Total iterations count: 4600
2022-06-01T06:22:55,822,+0000|INFO ||concord-bft.tls.connMgr||||TlsConnectionManager.cpp:57|Stopping connection manager for 4
2022-06-01T06:22:55,822,+0000|INFO ||simpletest.client||||simple_test_client.hpp:220|clientMetrics::retransmissions 18
2022-06-01T06:22:55,822,+0000|INFO ||simpletest.client||||simple_test_client.hpp:223|clientMetrics::retransmissionTimer 50
2022-06-01T06:22:55,822,+0000|INFO ||simpletest.client||||simple_test_client.hpp:236|test done, iterations: 4600

Client is done, killing 'parallel' at PID 68

parallel: SIGTERM received. No new jobs will be started.
parallel: Waiting for these 4 jobs to finish. Send SIGTERM again to stop now.
parallel: /concord-bft/build/tests/simpleTest/scripts/../server 1
parallel: /concord-bft/build/tests/simpleTest/scripts/../server 2
parallel: /concord-bft/build/tests/simpleTest/scripts/../server 3
parallel: /concord-bft/build/tests/simpleTest/scripts/../server 0

Killing server processes named '/concord-bft/build/tests/simpleTest/scripts/../server'
root@9cf36e02c374:/concord-bft#
root@9cf36e02c374:/concord-bft#

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
`<CONCORD_BFT_DIR>/build/tests/simpleTest/scripts`.

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
