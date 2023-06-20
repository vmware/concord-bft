## Project overview

The aim of this sub-project is to both document and to formally specify the concord-bft state machine replication (SMR) protocol. 

Given the distributed nature of the concord-bft protocol, unit tests offer limited means of establishing the desired state machine replication (SMR) safety properties. The main tool we use for proving correctness is an integration testing framework code-named [Apollo](https://github.com/vmware/concord-bft/tree/master/tests/apollo) which creates mini-blockchain deployments and exercises various runtime scenarios (including straight case, replica failures, network faults, as well as certain byzantine cases).

Apollo does give us good confidence as to the correctness and fault tolerance of the concord-bft SMR. But even if we bring test coverage to 100%, we can never be sure that we have discovered all possible bugs and failure scenarios.

We use Dafny and first-order logic to model and prove the safety properties of the protocol's core state machine (liveness properties are out of scope for now).

## Dev instructions

The Dafny model can be used as a document, for proving SBFT correctness properties, or as input for property-based tests of the implementation's core components.

## Install Docker itself if you don't have it already.

  * [Mac installer](https://docs.docker.com/v17.12/docker-for-mac/install/)

  * [Windows installer](https://docs.docker.com/v17.12/docker-for-windows/install/)

  * On linux:

```bash
sudo apt install docker.io
sudo service docker restart
sudo addgroup $USER docker
newgrp docker
```

## Pull the image.

(This is the slow thing; it includes a couple GB of Ubuntu.)

```bash
docker pull jonhdotnet/summer_school:1.1
```

## Run the image

Run the image connected to your filesystem so you can edit in your OS, and then run Dafny from inside the docker container:

```bash
mkdir work
cd work
docker container run -t -i --mount src="`pwd`",target=/home/dafnyserver/work,type=bind --workdir /home/dafnyserver/work jonhdotnet/summer_school:1.1 /bin/bash
git clone https://github.com/vmware/concord-bft
cd concord-bft/docs/sbft-formal-model
```

Now you can edit files using your preferred native OS editor under the work/
directory, and verify them with Dafny from the terminal that's running the
docker image.

The docker-based command-line Dafny installation above is offered as a
portable, simple way to get started.  There do exist snazzy real-time Dafny
integrations for IDEs (Visual Studio, VSCode) and editors (Emacs, Vim).  You
are certainly welcome to install Dafny natively and integrate it with your
editor if you prefer.

## Test that the image works

From the container started as described in the previous step run:

```bash
dafny /vcsCores:$(nproc) proof.dfy
```

If everything is working, you should see something like:

```bash
Dafny program verifier finished with 10 verified, 0 errors
```

## Acknowledgements and references

Special thanks to [@jonhnet](https://github.com/jonhnet) for his help and support along the way.

## Current status

# We have completed the modeling of *replica.i.dfy* covering the following parts of the protocol:

1. Slow Commit Path - the 2 phase commit path that requires collecting Prepared Certificate (2F+1 matching Prepares) and a Committed Certificate (2F+1 Commit messages).
2. View change - the mechanism that the system uses to replace the leader (Primary) in order to preserve livenes.
3. Sliding of the Working Window and Checkpointing - periodically the replicas exchange Checkpoint messages to reach agreement on the state. This also enables them to slide the Working Window of Sequence ID-s that can be considered for consensus in each replica. The checkpointing is crucial also for enabling replicas to catch up in case of long down time of a given replica. The Checkpoint represents a summary of the state (a hash in the implementation code and the entire state itself in the model) that can be used for correctness check when performing State Transfer.
4. State Transfer - the mechanism for replicas to help peers that have been disconnected for long enough to be unable to catch up because the system's Working Window has advanced beyond the particular replica's Working Window. This is a process that in the implementation code transfers portions of the state until catching up is completed in the behind replica. In the model we have the entire state in each Checkpoint message, therefore we can perform State Transfer once we collect quorum (2F+1) matching Checkpoint messages to the state in any of those messages (they have to be equal) if this state turns out to be more recent than our current state. We determine recentness based to the highest Sequence ID for which the system has reached consensus.

# Modeling of malicious behavior in the system *faulty_replica.i.dfy*.

To achieve validation in the presence of malicious behavior once we start writing the proof we needed a subset of the replicas (up to F) to act in a way that could try to corrupt the system. We achieve this by not restricting the F faulty replicas' actions in any specific way, thus allowing completely arbitrary behavior which can be considered as a super set of the Malicious behavior, which itself is a super set of the correct (honest) behavior that follows the protocol steps.

# Message signatures checks support in *network.s.dfy*.

In order for us to model signatures we have used the network to be a trusted arbitrary. It stores all the previously sent messages and can determine if a message really originated form a given host. The network rejects messages from hosts that try to impersonate another one. We also provide a mechanism to validate sub messages (some of the protocol messages are composite - carrying messages collected form peers in the model and hashes of those messages in the implementation code).

# Progress on the proof.

We have completed the proof that commit messages form honest senders agree in a given View, put otherwise, that consensus is reached in a View. This is a stepping stone for us to be able to prove consensus in the system is maintained even in the presence of multiple View Changes and at most F faulty replicas. The next milestone in proving UnCommitableAgreesWithPrepare. This predicate serves to bind the future and the past, showing that if an Hones Replica sends a Prepare message in a given View (effectively voting for a proposal from the primary for assigning a Client operation to a Sequence ID), it has performed all the necessary checks to validate that nothing else is commitable in prior views.
For the current state of the proof we have disabled sliding of the Working Window and Checkpointing in order to split the effort in smaller steps.