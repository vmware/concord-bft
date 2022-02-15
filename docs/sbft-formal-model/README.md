# Project overview
[SBFT](https://arxiv.org/pdf/1804.01626.pdf) is a byzantine fault-tolerant protocol (BFT) for state machine replication (SMR) designed and implemented in-house at VMware. SBFT is implemented in the [concord-bft](https://github.com/vmware/concord-bft) open source project. It is the foundation on which the [VMware Blockchain](https://docs.vmware.com/en/VMware-Blockchain/1.3/getting_started/GUID-6BD4CD5F-6AC6-4B05-BCB3-A76626BB2777.html) product has been built. A layered architecture enables integration with various smart contract execution engines, the currently supported ones being DAML and Ethereum’s EVM. We can think of VMware Blockchain as a new type of virtualized infrastructure that provides something beyond compute, a human value, which is trust.

Given the distributed nature of VMware Blockchain, unit tests offer limited means of establishing the correctness of the SMR protocol. The main tool we use for proving correctness is an integration testing framework code-named [Apollo](https://github.com/vmware/concord-bft/tree/master/tests/apollo) which creates mini-blockchain deployments and exercises various runtime scenarios (including straight case, replica failures, network faults, as well as certain byzantine cases).

Apollo does give us good confidence as to the correctness and fault tolerance of the system that we sell to our customers. But even if we bring test coverage to 100%, we can never be sure that we have discovered all possible bugs and failure scenarios, especially in a complex distributed system, such as VMware Blockchain.

The aim of this open source project is to reach beyond automated testing, into formal verification territory. This means using first-order logic to model and prove properties of the protocol. We are developing a formal model of the SBFT protocol using the Dafny language and framework, to formally prove interesting correctness properties of the SBFT consensus algorithm.

# Dev instructions

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
git clone https://github.com/HristoStaykov/sbft-formal-verification-model.git
cd sbft-formal-verification-model
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

# Acknowledgements and references

This project builds on top of the concepts and framework described in
https://github.com/GLaDOS-Michigan/summer-school-2021<br>

Special thanks to Jon Howell for his help and support along the way.
