# Project overview
The aim of this sub-project is to both document and to formally specify the concord-bft state machine replication (SMR) protocol. 

Given the distributed nature of the concord-bft protocol, unit tests offer limited means of establishing the desired state machine replication (SMR) safety properties. The main tool we use for proving correctness is an integration testing framework code-named [Apollo](https://github.com/vmware/concord-bft/tree/master/tests/apollo) which creates mini-blockchain deployments and exercises various runtime scenarios (including straight case, replica failures, network faults, as well as certain byzantine cases).

Apollo does give us good confidence as to the correctness and fault tolerance of the concord-bft SMR. But even if we bring test coverage to 100%, we can never be sure that we have discovered all possible bugs and failure scenarios.

We use Dafny and first-order logic to model and prove the safety properties of the protocol's core state machine (liveness properties are out of scope for now).

# Dev instructions

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

Special thanks to [@jonhnet|https://github.com/jonhnet] for his help and support along the way.
