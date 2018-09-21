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

If you have built Concord, by running `make.sh` from the top-level
directory, you can run the test using the following commands:

```
cd ~/builds/concord-bft/debug/bftengine/tests/simpleTest/scripts
./testReplicasAndClient.sh
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
